# Copyright Afam-Ifediogor, U. Divine
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Instrument `streaQ`_ to trace streaQ applications.

.. _streaQ: https://pypi.org/project/streaq/

Usage
-----

* Start Redis server (required by streaQ)

.. code::

    docker run -p 6379:6379 redis


* Create a worker module (worker.py)

.. code:: python

    from streaq import Worker
    from opentelemetry.instrumentation.streaq import StreaqInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    # Instrument streaQ
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    StreaqInstrumentor().instrument(tracer_provider=tracer_provider)

    worker = Worker(redis_url="redis://localhost")

    @worker.task
    async def my_task(data: str) -> str:
        return f"Processed: {data}"


* Run the worker

.. code::

    streaq run worker:worker


* Queue a task (script.py)

.. code:: python

    from anyio import run
    from worker import worker, my_task

    async def main():
        async with worker:
            await my_task.enqueue("hello")

    run(main)


Setting up tracing
-------------------

When tracing a streaQ worker, ensure instrumentation is initialized before
the worker starts. This is typically done at module import time as shown above.

The instrumentation automatically handles:

- **Producer spans**: Created when tasks are enqueued
- **Consumer spans**: Created when tasks are executed by workers
- **Context propagation**: Trace context is propagated from producers to consumers

API
---
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Iterator
from contextlib import contextmanager
from contextvars import Token
from datetime import datetime, timedelta, timezone
from typing import Any

import wrapt
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import is_instrumentation_enabled, unwrap
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import SpanKind, Tracer
from opentelemetry.trace.status import Status, StatusCode

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.streaq.attributes import (
    CompletionAttributes,
    ConsumerAttributes,
    ProducerAttributes,
)
from opentelemetry.instrumentation.streaq.package import _instruments
from opentelemetry.instrumentation.streaq.utils import (
    StreaqMetadataGetter,
    extract_metadata,
    inject_metadata,
)
from opentelemetry.instrumentation.streaq.version import __version__

logger: logging.Logger = logging.getLogger(__name__)


@contextmanager
def _attached_context(
    parent_context: context_api.Context | None,
) -> Iterator[None]:
    token: Token | None = None
    if parent_context is not None:
        token = context_api.attach(parent_context)
    try:
        yield
    finally:
        if token is not None:
            context_api.detach(token)


class StreaqInstrumentor(BaseInstrumentor):
    """Instrumentor for streaQ."""

    _patched: bool = False
    _tracer: Tracer | None = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider: Any = kwargs.get("tracer_provider")

        self._tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        self._patch_streaq()

    def _uninstrument(self, **kwargs: Any) -> None:
        self._unpatch_streaq()

    def _patch_streaq(self) -> None:
        if self._patched:
            return

        try:
            from streaq.task import AsyncRegisteredTask, SyncRegisteredTask
            from streaq.worker import Worker
        except ImportError:
            logger.warning("streaq not found, instrumentation will not work")
            return

        wrapt.wrap_function_wrapper(AsyncRegisteredTask, "enqueue", self._enqueue_wrapper)
        wrapt.wrap_function_wrapper(SyncRegisteredTask, "enqueue", self._enqueue_wrapper)
        wrapt.wrap_function_wrapper(Worker, "run_task", self._run_task_wrapper)

        self._patched = True

    def _unpatch_streaq(self) -> None:
        if not self._patched:
            return

        try:
            from streaq.task import AsyncRegisteredTask, SyncRegisteredTask
            from streaq.worker import Worker
        except ImportError:
            return

        unwrap(AsyncRegisteredTask, "enqueue")
        unwrap(SyncRegisteredTask, "enqueue")
        unwrap(Worker, "run_task")

        self._patched = False

    @staticmethod
    def _to_ms(val: Any) -> int | None:
        if val is None:
            return None
        if isinstance(val, timedelta):
            return int(val.total_seconds() * 1000)
        return int(float(val) * 1000)

    @staticmethod
    def _timestamp_ms_to_iso(ms: Any) -> str:
        if not ms:
            return "unknown"
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).isoformat()

    def _set_producer_attributes(
        self,
        span: trace.Span,
        instance: Any,
        task: Any,
        destination: str,
        priority: str,
    ) -> None:
        task_schedule: Any = getattr(task, "schedule", None)
        crontab: str | None = getattr(instance, "crontab", None)
        scheduled_time: str | None = None

        if isinstance(task_schedule, str):
            crontab = task_schedule
        elif isinstance(task_schedule, datetime):
            scheduled_time = task_schedule.isoformat()

        ProducerAttributes(
            destination=destination,
            task_id=str(getattr(task, "id", "unknown")),
            task_function=str(getattr(instance, "fn_name", "unknown")),
            task_priority=str(priority),
            max_retries=getattr(instance, "max_tries", None),
            timeout_ms=self._to_ms(getattr(instance, "timeout", None)),
            ttl_ms=self._to_ms(getattr(instance, "ttl", None)),
            delay_ms=self._to_ms(getattr(task, "delay", None)),
            expire_ms=self._to_ms(getattr(instance, "expire", None)),
            unique=getattr(instance, "unique", None),
            dependencies=getattr(task, "after", None),
            crontab=crontab,
            scheduled_time=scheduled_time,
        ).set(span)

    def _set_consumer_attributes(
        self, span: trace.Span, worker: Any, msg: Any, destination: str, priority: str
    ) -> None:
        priorities: list[str] = getattr(worker, "priorities", [])
        priorities_str: str = ",".join(reversed(priorities)) if priorities else ""
        enqueue_time_iso: str = self._timestamp_ms_to_iso(getattr(msg, "enqueue_time", None))

        ConsumerAttributes(
            destination=destination,
            message_id=str(getattr(msg, "message_id", "unknown")),
            consumer_id=str(getattr(worker, "id", "unknown")),
            worker_concurrency=getattr(worker, "concurrency", 1),
            worker_priorities=priorities_str,
            task_id=str(getattr(msg, "task_id", "unknown")),
            task_function=str(getattr(msg, "fn_name", getattr(msg, "task_name", "unknown"))),
            task_priority=str(priority),
            retry_count=getattr(msg, "tries", 0),
            enqueue_time=enqueue_time_iso,
            worker_sync_concurrency=getattr(worker, "sync_concurrency", None),
        ).set(span)

    def _set_completion_attributes(self, span: trace.Span, msg: Any, result: Any) -> None:
        start_time: float | int | None = getattr(result, "start_time", None)
        finish_time: float | int | None = getattr(result, "finish_time", None)

        duration: float | int = 0
        if start_time is not None and finish_time is not None:
            duration = finish_time - start_time

        CompletionAttributes(
            success=bool(getattr(result, "success", True)),
            execution_duration_ms=int(duration),
            start_time=self._timestamp_ms_to_iso(start_time),
            finish_time=self._timestamp_ms_to_iso(finish_time),
            enqueue_time=self._timestamp_ms_to_iso(
                getattr(result, "enqueue_time", getattr(msg, "enqueue_time", None))
            ),
            result_ttl=self._to_ms(getattr(result, "ttl", None)),
        ).set(span)

    def _enqueue_wrapper(
        self,
        wrapped: Callable[..., Any],
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled() or self._tracer is None:
            return wrapped(*args, **kwargs)

        task: Any = wrapped(*args, **kwargs)
        if task is None:
            return task

        worker: Any = getattr(instance, "worker", None)
        queue_name: str = getattr(worker, "queue_name", "default")

        priority: str | None = getattr(task, "priority", None)
        if not priority and hasattr(worker, "priorities") and worker.priorities:
            priority = worker.priorities[-1]
        priority = priority or "default"

        destination: str = f"{queue_name}:{priority}"

        with self._tracer.start_as_current_span(
            f"{destination} publish",
            kind=SpanKind.PRODUCER,
        ) as span:
            self._set_producer_attributes(span, instance, task, destination, priority)

            if hasattr(task, "kwargs"):
                if task.kwargs is None:
                    task.kwargs = {}
                carrier: dict[str, str] = {}
                inject(carrier)
                inject_metadata(task.kwargs, carrier)

        return task

    async def _run_task_wrapper(
        self,
        wrapped: Callable[..., Any],
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled() or self._tracer is None:
            return await wrapped(*args, **kwargs)

        msg: Any = kwargs.get("msg") or (args[0] if args else None)
        if msg is None:
            return await wrapped(*args, **kwargs)

        worker: Any = instance
        priority: str = getattr(msg, "priority", "default")
        destination: str = f"{getattr(worker, 'queue_name', 'default')}:{priority}"

        carrier: dict[str, Any] = getattr(msg, "kwargs", {})
        if not carrier and hasattr(msg, "data") and isinstance(msg.data, dict):
            carrier = msg.data.get("kwargs", {})

        metadata: dict[str, Any] = extract_metadata(carrier)
        parent_context: context_api.Context | None = extract(
            metadata, getter=StreaqMetadataGetter()
        )

        with (
            # Attach the parent context to the current context
            _attached_context(parent_context),
            # Start a new span with the parent context
            self._tracer.start_as_current_span(
                f"{destination} process",
                kind=SpanKind.CONSUMER,
            ) as span,
        ):
            self._set_consumer_attributes(span, worker, msg, destination, priority)

            try:
                result: Any = await wrapped(*args, **kwargs)
                span.set_status(Status(StatusCode.OK))

                if result:
                    self._set_completion_attributes(span, msg, result)

                return result
            except Exception as exc:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
                raise
