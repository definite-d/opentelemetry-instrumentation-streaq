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

import logging
from typing import Any, Collection

import wrapt
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import is_instrumentation_enabled, unwrap
from opentelemetry.propagate import extract, inject
from opentelemetry.trace.status import Status, StatusCode

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.streaq.package import _instruments
from opentelemetry.instrumentation.streaq.utils import (
    SpanAttributes,
    StreaqMetadataGetter,
    extract_metadata,
    inject_metadata,
    set_span_attributes_from_task,
)
from opentelemetry.instrumentation.streaq.version import __version__

logger = logging.getLogger(__name__)


class _OpenTelemetryMiddleware:
    def __init__(self, tracer: trace.Tracer):
        self._tracer = tracer

    def __call__(self, func):

        async def wrapper(*args, **kwargs):
            if not is_instrumentation_enabled():
                return await func(*args, **kwargs)

            # Extract trace context from kwargs
            metadata = extract_metadata(kwargs)
            parent_context = extract(metadata, getter=StreaqMetadataGetter())

            # Attach parent context if present
            token = None
            if parent_context:
                token = context_api.attach(parent_context)
                logger.debug("Extracted trace context for task execution")

            try:
                # Get task info from context if available
                task_context = kwargs.get("context")
                task_name = getattr(task_context, "fn_name", func.__name__)
                task_id = getattr(task_context, "task_id", "unknown")
                queue_name = "default"
                retry_count = getattr(task_context, "tries", 0)

                # Create consumer span with parent context
                with self._tracer.start_as_current_span(
                    f"{task_name} process",
                    context=parent_context,
                    kind=trace.SpanKind.CONSUMER,
                ) as span:
                    # Set span attributes
                    set_span_attributes_from_task(
                        span,
                        task_name=task_name,
                        task_id=task_id,
                        queue_name=queue_name,
                        retry_count=retry_count,
                    )
                    span.set_attribute(SpanAttributes.MESSAGING_OPERATION, "process")

                    try:
                        result = await func(*args, **kwargs)
                        return result
                    except Exception as exc:
                        if span.is_recording():
                            span.set_status(Status(StatusCode.ERROR, str(exc)))
                            span.record_exception(exc)
                        raise
            finally:
                if token is not None:
                    context_api.detach(token)

        return wrapper


class StreaqInstrumentor(BaseInstrumentor):
    _instance = None
    _patched = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        """Enable instrumentation for streaQ.

        Args:
            tracer_provider: Optional custom tracer provider
        """
        tracer_provider = kwargs.get("tracer_provider")

        # pylint: disable=attribute-defined-outside-init
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

        # Patch enqueue methods (producer side)
        wrapt.wrap_function_wrapper(
            AsyncRegisteredTask, "enqueue", self._enqueue_wrapper
        )
        wrapt.wrap_function_wrapper(
            SyncRegisteredTask, "enqueue", self._enqueue_wrapper
        )

        # Patch Worker.__init__ to auto-add middleware (consumer side)
        wrapt.wrap_function_wrapper(Worker, "__init__", self._worker_init_wrapper)

        self._patched = True
        logger.debug("streaQ instrumentation patched")

    def _unpatch_streaq(self) -> None:
        if not self._patched:
            return

        try:
            from streaq.task import AsyncRegisteredTask, SyncRegisteredTask
            from streaq.worker import Worker
        except ImportError:
            return

        # Use OpenTelemetry's unwrap utility to restore originals
        unwrap(AsyncRegisteredTask, "enqueue")
        unwrap(SyncRegisteredTask, "enqueue")
        unwrap(Worker, "__init__")

        self._patched = False
        logger.debug("streaQ instrumentation unpatched")

    def _worker_init_wrapper(self, wrapped, instance, args, kwargs):
        result = wrapped(*args, **kwargs)

        already_added = any(
            isinstance(m, _OpenTelemetryMiddleware) for m in instance.middlewares
        )
        if not already_added:
            middleware = _OpenTelemetryMiddleware(tracer=self._tracer)
            instance.middlewares.append(middleware)
            logger.debug("Added OpenTelemetry middleware to Worker")

        return result

    def _enqueue_wrapper(self, wrapped, instance, args, kwargs):
        if not is_instrumentation_enabled():
            return wrapped(*args, **kwargs)

        task_name = getattr(instance, "fn_name", str(wrapped))
        queue_name = getattr(instance.worker, "queue_name", "default")

        with self._tracer.start_as_current_span(
            f"{task_name} publish",
            kind=trace.SpanKind.PRODUCER,
        ) as span:
            set_span_attributes_from_task(
                span,
                task_name=task_name,
                task_id="pending",
                queue_name=queue_name,
            )
            span.set_attribute(SpanAttributes.MESSAGING_OPERATION, "publish")

            result = wrapped(*args, **kwargs)

            if result is not None and hasattr(result, "kwargs"):
                carrier: dict[str, str] = {}
                inject(carrier)
                inject_metadata(result.kwargs, carrier)
                logger.debug("Injected trace context into task %s", task_name)

                task_id = getattr(result, "id", None)
                if task_id and span.is_recording():
                    span.set_attribute(SpanAttributes.MESSAGING_MESSAGE_ID, task_id)

            return result
