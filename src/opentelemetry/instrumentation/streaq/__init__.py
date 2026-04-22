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
from opentelemetry.propagate import inject

from opentelemetry import trace
from opentelemetry.instrumentation.streaq.package import _instruments
from opentelemetry.instrumentation.streaq.utils import (
    inject_metadata,
    set_span_attributes_from_task,
)
from opentelemetry.instrumentation.streaq.version import __version__

logger = logging.getLogger(__name__)


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
        except ImportError:
            logger.warning("streaq not found, instrumentation will not work")
            return

        wrapt.wrap_function_wrapper(
            AsyncRegisteredTask, "enqueue", self._enqueue_wrapper
        )
        wrapt.wrap_function_wrapper(
            SyncRegisteredTask, "enqueue", self._enqueue_wrapper
        )

        self._patched = True
        logger.debug("streaQ instrumentation patched")

    def _unpatch_streaq(self) -> None:
        if not self._patched:
            return

        try:
            from streaq.task import AsyncRegisteredTask, SyncRegisteredTask
        except ImportError:
            return

        # Use OpenTelemetry's unwrap utility to restore originals
        unwrap(AsyncRegisteredTask, "enqueue")
        unwrap(SyncRegisteredTask, "enqueue")

        self._patched = False
        logger.debug("streaQ instrumentation unpatched")

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
            span.set_attribute("messaging.operation", "publish")

            result = wrapped(*args, **kwargs)

            if result is not None and hasattr(result, "kwargs"):
                carrier: dict[str, str] = {}
                inject(carrier)
                inject_metadata(result.kwargs, carrier)
                logger.debug("Injected trace context into task %s", task_name)

                task_id = getattr(result, "id", None)
                if task_id and span.is_recording():
                    span.set_attribute("messaging.message.id", task_id)

            return result
