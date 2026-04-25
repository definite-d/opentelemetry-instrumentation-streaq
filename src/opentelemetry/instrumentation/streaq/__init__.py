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

    _tracer: Tracer | None = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider: Any = kwargs.get("tracer_provider")
        schema_url: str | None = kwargs.get("schema_url")

        self._tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url=schema_url,
        )

        self._patch_streaq()

    def _uninstrument(self, **kwargs: Any) -> None:
        self._unpatch_streaq()

    def _patch_streaq(self) -> None:
        try:
            from streaq.task import Task
            from streaq.worker import Worker
        except ImportError:
            logger.warning("streaq not found, instrumentation will not work")
            return

        wrapt.wrap_function_wrapper(Task, "_enqueue", self._enqueue_wrapper)
        wrapt.wrap_function_wrapper(Worker, "run_task", self._run_task_wrapper)

    def _unpatch_streaq(self) -> None:
        try:
            from streaq.task import Task
            from streaq.worker import Worker
        except ImportError:
            return

        unwrap(Task, "_enqueue")
        unwrap(Worker, "run_task")

    @staticmethod
    def _to_ms(val: Any) -> int | None:
        if val is None:
            return None
        if isinstance(val, timedelta):
            return int(val.total_seconds() * 1000)
        return int(float(val) * 1000)

    @staticmethod
    def _timestamp_ms_to_iso(ms: Any) -> str | None:
        if not ms:
            return None
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).isoformat()

    def _set_producer_attributes(self, span: trace.Span, task: Any, destination: str, priority: str) -> None:
        # Extract parent attributes (not available on Task)
        parent: Any = task.parent
        crontab: str | None = None
        delay_ms: int | None = self._to_ms(getattr(task, "delay", None))
        dependencies: list[str] | None = getattr(task, "after", None)
        expire_ms: int | None = self._to_ms(getattr(parent, "expire", None))
        fn_name: str = str(getattr(parent, "fn_name", "unknown"))
        max_retries: int | None = getattr(parent, "max_tries", None)
        scheduled_time: str | None = None
        task_id: str = str(getattr(task, "id", "unknown"))
        task_schedule: Any = getattr(task, "schedule", None)
        timeout_ms: int | None = self._to_ms(getattr(parent, "timeout", None))
        ttl_ms: int | None = self._to_ms(getattr(parent, "ttl", None))
        unique: bool | None = getattr(parent, "unique", None)

        if isinstance(task_schedule, str):
            crontab = task_schedule
        elif isinstance(task_schedule, datetime):
            scheduled_time = task_schedule.isoformat()

        ProducerAttributes(
            crontab=crontab,
            dependencies=dependencies,
            delay_ms=delay_ms,
            destination=destination,
            expire_ms=expire_ms,
            max_retries=max_retries,
            scheduled_time=scheduled_time,
            task_function=fn_name,
            task_id=task_id,
            task_priority=str(priority),
            timeout_ms=timeout_ms,
            ttl_ms=ttl_ms,
            unique=unique,
        ).set(span)

    def _set_consumer_attributes(self, span: trace.Span, worker: Any, msg: Any, destination: str) -> None:
        consumer_id: str = str(getattr(worker, "id", "unknown"))
        enqueue_time_iso: str | None = self._timestamp_ms_to_iso(getattr(msg, "enqueue_time", None))
        message_id: str = str(getattr(msg, "message_id", "unknown"))
        priorities: list[str] = getattr(worker, "priorities", [])
        priorities_str: str = ",".join(reversed(priorities)) if priorities else ""
        priority: str = getattr(msg, "priority", "default")
        task_function: str = str(getattr(msg, "fn_name", "unknown"))
        task_id: str = str(getattr(msg, "task_id", "unknown"))
        timeout_ms: int | None = self._to_ms(getattr(msg, "timeout", None))
        worker_concurrency: int = getattr(worker, "concurrency", 1)
        worker_sync_concurrency: int | None = getattr(worker, "sync_concurrency", None)

        ConsumerAttributes(
            consumer_id=consumer_id,
            destination=destination,
            enqueue_time=enqueue_time_iso,
            message_id=message_id,
            retry_count=getattr(msg, "tries", 0),
            task_function=task_function,
            task_id=task_id,
            task_priority=str(priority),
            timeout_ms=timeout_ms,
            worker_concurrency=worker_concurrency,
            worker_priorities=priorities_str,
            worker_sync_concurrency=worker_sync_concurrency,
        ).set(span)

    def _set_completion_attributes(self, span: trace.Span, msg: Any, result: Any) -> None:
        enqueue_time: float | int | None = getattr(result, "enqueue_time", getattr(msg, "enqueue_time", None))
        enqueue_time_iso: str | None = self._timestamp_ms_to_iso(enqueue_time)
        execution_duration_ms: int = 0
        finish_time: float | int | None = getattr(result, "finish_time", None)
        finish_time_iso: str | None = self._timestamp_ms_to_iso(finish_time)
        result_ttl: int | None = self._to_ms(getattr(result, "ttl", None))
        start_time: float | int | None = getattr(result, "start_time", None)
        start_time_iso: str | None = self._timestamp_ms_to_iso(start_time)
        success: bool = bool(getattr(result, "success", True))

        if start_time is not None and finish_time is not None:
            execution_duration_ms = int(finish_time - start_time)

        CompletionAttributes(
            enqueue_time=enqueue_time_iso,
            execution_duration_ms=execution_duration_ms,
            finish_time=finish_time_iso,
            result_ttl=result_ttl,
            start_time=start_time_iso,
            success=success,
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

        task: Any = instance
        worker: Any = getattr(task, "worker", None)
        queue_name: str = getattr(worker, "queue_name", "default")
        priority: str = getattr(task, "priority", None) or getattr(worker, "priorities", ["default"])[-1]
        destination: str = f"{queue_name}:{priority}"

        with self._tracer.start_as_current_span(
            f"{destination} publish",
            kind=SpanKind.PRODUCER,
        ) as span:
            # Inject trace context into task kwargs before serialization
            if hasattr(task, "kwargs"):
                if task.kwargs is None:
                    task.kwargs = {}
                carrier: dict[str, str] = {}
                inject(carrier)
                inject_metadata(task.kwargs, carrier)

            # Call the original _enqueue method
            result: Any = wrapped(*args, **kwargs)

            # Set producer attributes
            self._set_producer_attributes(span, task, destination, priority)

        return result

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
            self._set_consumer_attributes(span, worker, msg, destination)

            try:
                result: Any = await wrapped(*args, **kwargs)
                span.set_status(Status(StatusCode.OK))
                self._set_completion_attributes(span, msg, result)
                return result
            except Exception as exc:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)
                raise
