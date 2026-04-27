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

import time
from unittest.mock import Mock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv.attributes.exception_attributes import (
    EXCEPTION_MESSAGE,
    EXCEPTION_STACKTRACE,
    EXCEPTION_TYPE,
)
from opentelemetry.trace import SpanKind, StatusCode

from opentelemetry.instrumentation.streaq import StreaqInstrumentor
from opentelemetry.instrumentation.streaq.utils import OTEL_METADATA_KEY


class TestStreaqInstrumentation:
    """Test streaq instrumentation integration."""

    async def test_enqueue_creates_producer_span(
        self, instrumentor, mock_instance, mock_task, memory_exporter
    ):
        """Enqueue creates a producer span with correct attributes."""

        async def mock_wrapped(*args, **kwargs):
            return mock_task

        await instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.kind == SpanKind.PRODUCER
        assert "publish" in span.name
        assert span.attributes["messaging.operation"] == "publish"
        assert span.attributes["messaging.system"] == "redis"
        assert span.attributes["messaging.destination.name"] == "normal"
        assert span.attributes["streaq.task.function"] == "test_task"

    async def test_enqueue_injects_context(self, instrumentor, mock_instance, mock_task):
        """Enqueue injects trace context into task kwargs."""

        async def mock_wrapped(*args, **kwargs):
            return mock_task

        await instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        assert OTEL_METADATA_KEY in mock_instance.kwargs
        assert "traceparent" in mock_instance.kwargs[OTEL_METADATA_KEY]

    async def test_uninstrument_removes_patches(
        self, instrumentor, mock_instance, mock_task, memory_exporter
    ):
        """Uninstrument removes patches and stops tracing."""
        instrumentor.uninstrument()

        async def mock_wrapped(*args, **kwargs):
            return mock_task

        instrumentor._tracer = None
        await instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0


class TestConsumerSpan:
    """Test consumer span creation via middleware."""

    async def test_middleware_creates_consumer_span(self, instrumentor, mock_msg, memory_exporter):
        """Middleware creates consumer span on task execution."""
        mock_ctx = Mock()
        mock_ctx.kwargs = {}
        mock_ctx.priority = "normal"
        mock_ctx.fn_name = "test_task"
        mock_ctx.tries = 0
        mock_ctx.task_id = "task-456"
        mock_ctx.timeout = None

        async def mock_task():
            return Mock(
                success=True,
                start_time=time.time(),
                finish_time=time.time(),
            )

        middleware = instrumentor._otel_middleware()
        await middleware(mock_task, ctx=mock_ctx)()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.kind == SpanKind.CONSUMER
        assert "process" in span.name

    async def test_middleware_records_exception(self, instrumentor, mock_msg, memory_exporter):
        """Middleware records exception details on failure."""
        mock_ctx = Mock()
        mock_ctx.kwargs = {}
        mock_ctx.priority = "normal"
        mock_ctx.fn_name = "failing_task"
        mock_ctx.tries = 0
        mock_ctx.task_id = "task-456"
        mock_ctx.timeout = None

        async def mock_task():
            raise ValueError("Task failed!")

        middleware = instrumentor._otel_middleware()

        with pytest.raises(ValueError):
            await middleware(mock_task, ctx=mock_ctx)()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.status.status_code == StatusCode.ERROR

        exception_events = [e for e in span.events if e.name == "exception"]
        assert len(exception_events) >= 1

        event = exception_events[0]
        assert EXCEPTION_STACKTRACE in event.attributes
        assert event.attributes[EXCEPTION_TYPE] == "ValueError"
        assert event.attributes[EXCEPTION_MESSAGE] == "Task failed!"

        exception_events = [e for e in span.events if e.name == "exception"]
        assert len(exception_events) >= 1

        event = exception_events[0]
        assert EXCEPTION_STACKTRACE in event.attributes
        assert event.attributes[EXCEPTION_TYPE] == "ValueError"
        assert event.attributes[EXCEPTION_MESSAGE] == "Task failed!"


class TestContextPropagation:
    """Test trace context propagation between producer and consumer."""

    async def test_trace_context_propagates(self, instrumentor, mock_instance, mock_task):
        """Trace context is propagated from producer to consumer."""

        async def mock_wrapped(*args, **kwargs):
            return mock_task

        await instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        assert OTEL_METADATA_KEY in mock_instance.kwargs
        assert "traceparent" in mock_instance.kwargs[OTEL_METADATA_KEY]


class TestErrorHandling:
    """Test error handling in instrumentation."""

    async def test_middleware_without_tracer(self, instrumentor, memory_exporter):
        """Middleware without tracer returns result without creating span."""
        mock_ctx = Mock()
        mock_ctx.kwargs = {}
        mock_ctx.priority = "normal"
        mock_ctx.fn_name = "test_task"
        mock_ctx.tries = 0
        mock_ctx.task_id = "task-123"
        mock_ctx.timeout = None

        async def mock_task():
            return "result"

        instrumentor._tracer = None
        result = await instrumentor._otel_middleware()(mock_task, ctx=mock_ctx)()

        assert result == "result"
        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0
