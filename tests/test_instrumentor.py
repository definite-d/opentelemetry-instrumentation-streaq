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

"""Tests for StreaqInstrumentor unit tests."""

from opentelemetry.trace import SpanKind

from opentelemetry.instrumentation.streaq import StreaqInstrumentor
from opentelemetry.instrumentation.streaq.utils import OTEL_METADATA_KEY


class TestProducerSpanCreation:
    """Test producer span creation with pytest fixtures."""

    def test_enqueue_creates_producer_span(
        self, instrumentor, mock_instance, mock_task, memory_exporter
    ):
        """Enqueue creates a producer span with correct attributes."""

        def mock_wrapped(*args, **kwargs):
            return mock_task

        instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.kind == SpanKind.PRODUCER
        assert "publish" in span.name
        assert span.attributes["messaging.operation"] == "publish"
        assert span.attributes["messaging.system"] == "redis"
        assert span.attributes["messaging.destination.name"] == "test_queue:default"
        assert span.attributes["streaq.task.function"] == "test_task"

    def test_enqueue_injects_context(self, instrumentor, mock_instance, mock_task):
        """Enqueue injects trace context into task kwargs."""

        def mock_wrapped(*args, **kwargs):
            return mock_task

        instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        assert OTEL_METADATA_KEY in mock_task.kwargs
        assert "traceparent" in mock_task.kwargs[OTEL_METADATA_KEY]


class TestDuplicateInstrumentation:
    """Test duplicate instrumentation handling."""

    def test_duplicate_instrumentation(self):
        """Multiple instrumentation calls should be idempotent."""
        first = StreaqInstrumentor()
        second = StreaqInstrumentor()

        first.instrument()
        initial_patched_state = first._patched

        second.instrument()

        assert first._patched == initial_patched_state
        assert first._patched == second._patched

        StreaqInstrumentor().uninstrument()

        assert not first._patched
        assert not second._patched
