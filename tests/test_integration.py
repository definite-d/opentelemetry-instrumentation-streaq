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
from opentelemetry.semconv.attributes.exception_attributes import (
    EXCEPTION_MESSAGE,
    EXCEPTION_STACKTRACE,
    EXCEPTION_TYPE,
)
from opentelemetry.trace import SpanKind, StatusCode

from opentelemetry.instrumentation.streaq.utils import OTEL_METADATA_KEY


class TestStreaqInstrumentation:
    """Test streaq instrumentation integration."""

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

    def test_uninstrument_removes_patches(
        self, instrumentor, mock_instance, mock_task, memory_exporter
    ):
        """Uninstrument removes patches and stops tracing."""
        instrumentor.uninstrument()

        def mock_wrapped(*args, **kwargs):
            return mock_task

        instrumentor._tracer = None
        instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0


class TestConsumerSpan:
    """Test consumer span creation and error handling."""

    async def test_consumer_span_on_task_execution(
        self, instrumentor, mock_worker, mock_msg, memory_exporter
    ):
        """Consumer span is created on task execution."""

        def mock_wrapped(*args, **kwargs):
            return Mock(
                success=True,
                start_time=time.time(),
                finish_time=time.time(),
                enqueue_time=time.time(),
                ttl=None,
            )

        await instrumentor._run_task_wrapper(mock_wrapped, mock_worker, (), {"msg": mock_msg})

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        span = spans[0]
        assert span.kind == SpanKind.CONSUMER
        assert "process" in span.name

    async def test_consumer_span_records_exception(
        self, instrumentor, mock_worker, mock_msg, memory_exporter
    ):
        """Consumer span records exception details on failure."""
        mock_msg.fn_name = "failing_task"

        def mock_wrapped(*args, **kwargs):
            raise ValueError("Task failed!")

        with pytest.raises(ValueError):
            await instrumentor._run_task_wrapper(mock_wrapped, mock_worker, (), {"msg": mock_msg})

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


class TestContextPropagation:
    """Test trace context propagation between producer and consumer."""

    def test_trace_context_propagates(self, instrumentor, mock_instance, mock_task):
        """Trace context is propagated from producer to consumer."""

        def mock_wrapped(*args, **kwargs):
            return mock_task

        instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        assert OTEL_METADATA_KEY in mock_task.kwargs
        assert "traceparent" in mock_task.kwargs[OTEL_METADATA_KEY]


class TestErrorHandling:
    """Test error handling in instrumentation."""

    def test_enqueue_with_none_task(self, instrumentor, mock_instance_with_worker, memory_exporter):
        """Enqueue with None task returns None without creating span."""

        def mock_wrapped(*args, **kwargs):
            return None

        result = instrumentor._enqueue_wrapper(mock_wrapped, mock_instance_with_worker, (), {})

        assert result is None
        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0

    async def test_run_task_with_none_msg(self, instrumentor, mock_worker, memory_exporter):
        """Run task with None msg returns result without creating span."""

        def mock_wrapped(*args, **kwargs):
            return "result"

        result = await instrumentor._run_task_wrapper(mock_wrapped, mock_worker, (), {})

        assert result == "result"
        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0
