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
from datetime import datetime, timedelta
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
        wrapt.wrap_function_wrapper(Worker, "__init__", self._init_wrapper)

    def _unpatch_streaq(self) -> None:
        try:
            from streaq.task import Task
            from streaq.worker import Worker
        except ImportError:
            return

        unwrap(Task, "_enqueue")
        unwrap(Worker, "__init__")

    @staticmethod
    def _to_ms(val: Any) -> int | None:
        if val is None:
            return None
        if isinstance(val, timedelta):
            return int(val.total_seconds() * 1000)
        return int(float(val) * 1000)

    def _set_producer_attributes(self, span: trace.Span, task: Any, destination: str) -> None:
        parent: Any = task.parent
        scheduled_time: str | None = None
        task_id: str = str(task.id)
        task_schedule: Any = task.schedule
        fn_name: str = str(parent.fn_name)
        timeout_ms: int | None = self._to_ms(parent.timeout)
        ttl_ms: int | None = self._to_ms(parent.ttl)

        if isinstance(task_schedule, datetime):
            scheduled_time = task_schedule.isoformat()

        ProducerAttributes(
            destination=destination,
            operation="publish",
            scheduled_time=scheduled_time,
            system="redis",
            task_function=fn_name,
            task_id=task_id,
            timeout_ms=timeout_ms,
            ttl_ms=ttl_ms,
        ).set(span)

    async def _enqueue_wrapper(
        self,
        wrapped: Callable[..., Any],
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled() or self._tracer is None:
            return await wrapped(*args, **kwargs)

        task: Any = instance
        worker: Any = task.worker
        destination: str = getattr(task, "priority", None) or worker.priorities[-1]

        with self._tracer.start_as_current_span(
            f"{destination} publish",
            kind=SpanKind.PRODUCER,
        ) as span:
            # Inject trace context into task kwargs before serialization
            if task.kwargs is None:
                task.kwargs = {}
            carrier: dict[str, str] = {}
            inject(carrier)
            inject_metadata(task.kwargs, carrier)

            # Call the original _enqueue method
            result: Any = await wrapped(*args, **kwargs)

            # Set producer attributes
            self._set_producer_attributes(span, task, destination)

        return result

    def _init_wrapper(
        self,
        wrapped: Callable[..., Any],
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        result = wrapped(*args, **kwargs)

        try:
            instance.middleware(self._otel_middleware())
        except ImportError:
            pass

        return result

    def _otel_middleware(self) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def middleware(
            task: Callable[..., Any],
            ctx: Any = None,
        ) -> Any:
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                if not is_instrumentation_enabled() or self._tracer is None:
                    return await task(*args, **kwargs)

                if ctx is None:
                    return await task(*args, **kwargs)

                destination: str = getattr(ctx, "priority", "normal")
                fn_name: str = getattr(ctx, "fn_name", "unknown")
                retry_count: int = getattr(ctx, "tries", 0)
                task_id: str = getattr(ctx, "task_id", "")
                timeout_ms: int | None = getattr(ctx, "timeout", None)

                metadata: dict[str, Any] = extract_metadata(ctx.kwargs)
                parent_context: context_api.Context | None = extract(
                    metadata, getter=StreaqMetadataGetter()
                )

                with (
                    _attached_context(parent_context),
                    self._tracer.start_as_current_span(
                        f"{destination} process",
                        kind=SpanKind.CONSUMER,
                    ) as span,
                ):
                    ConsumerAttributes(
                        destination=destination,
                        operation="process",
                        retry_count=retry_count,
                        system="redis",
                        task_function=fn_name,
                        task_id=task_id,
                        timeout_ms=timeout_ms,
                    ).set(span)

                    start_time: float | None = None
                    end_time: float | None = None
                    success: bool = True

                    try:
                        result = await task(*args, **kwargs)
                        span.set_status(Status(StatusCode.OK))

                        if hasattr(result, "start_time"):
                            start_time = result.start_time
                        if hasattr(result, "finish_time"):
                            end_time = result.finish_time
                        if hasattr(result, "success"):
                            success = result.success

                        execution_duration_ms: int = 0
                        if start_time is not None and end_time is not None:
                            execution_duration_ms = int(end_time - start_time)

                        CompletionAttributes(
                            execution_duration_ms=execution_duration_ms,
                            success=success,
                        ).set(span)

                        return result
                    except Exception as exc:
                        span.set_status(Status(StatusCode.ERROR, str(exc)))
                        span.record_exception(exc)
                        raise

            return wrapper

        return middleware
