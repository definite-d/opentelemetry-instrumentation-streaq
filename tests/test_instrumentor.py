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

from datetime import datetime, timedelta
from unittest.mock import Mock

from opentelemetry.trace import SpanKind

from opentelemetry.instrumentation.streaq.utils import OTEL_METADATA_KEY


class TestProducerSpanCreation:
    """Test producer span creation with pytest fixtures."""

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
        assert "send" in span.name
        assert span.attributes["messaging.operation.type"] == "send"
        assert span.attributes["messaging.system"] == "redis"
        assert span.attributes["messaging.destination.name"] == "normal"
        assert span.attributes["messaging.operation.name"] == "test_task"

    async def test_enqueue_injects_context(self, instrumentor, mock_instance, mock_task):
        """Enqueue injects trace context into task kwargs."""

        async def mock_wrapped(*args, **kwargs):
            return mock_task

        await instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        assert OTEL_METADATA_KEY in mock_instance.kwargs
        assert "traceparent" in mock_instance.kwargs[OTEL_METADATA_KEY]

    async def test_enqueue_with_none_kwargs_is_handled(
        self, instrumentor, mock_instance, mock_task, memory_exporter
    ):
        """kwargs=None is handled by setting to empty dict before injection."""
        mock_instance.kwargs = None

        async def mock_wrapped(*args, **kwargs):
            return mock_task

        await instrumentor._enqueue_wrapper(mock_wrapped, mock_instance, (), {})

        assert isinstance(mock_instance.kwargs, dict)
        assert OTEL_METADATA_KEY in mock_instance.kwargs

    async def test_enqueue_with_datetime_schedule_sets_scheduled_time(
        self, instrumentor, mock_worker, memory_exporter
    ):
        """Datetime schedule sets scheduled_time attribute."""
        task = Mock()
        task.id = "task-scheduled"
        task.kwargs = {}
        task.priority = "normal"
        task.delay = None
        task.schedule = datetime(2026, 12, 31, 23, 59, 59)
        task.after = []
        task.worker = mock_worker
        parent = Mock()
        parent.fn_name = "test_task"
        parent.expire = None
        parent.max_tries = 3
        parent.timeout = 30000
        parent.ttl = 3600000
        parent.unique = False
        task.parent = parent

        async def mock_wrapped(*args, **kwargs):
            return task

        await instrumentor._enqueue_wrapper(mock_wrapped, task, (), {})

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.attributes["streaq.task.scheduled_time"] == "2026-12-31T23:59:59"


class TestInitWrapper:
    """Test _init_wrapper method."""

    async def test_init_wrapper_stores_server_address_and_registers_middleware(self, instrumentor):
        """_init_wrapper extracts server address and registers middleware."""
        instance = Mock()
        instance.id = "test-worker"
        instance.middlewares = []

        def fake_middleware(fn):
            instance.middlewares.append(fn)
            return fn

        instance.middleware = fake_middleware

        wrapped = Mock()
        wrapped.return_value = None

        instrumentor._init_wrapper(
            wrapped, instance, (), {"redis_url": "valkey://redis-prod:6380/11"}
        )

        wrapped.assert_called_once()
        assert instance._otel_server_address == "redis-prod"
        assert instance._otel_server_port == 6380
        assert len(instance.middlewares) == 1

    async def test_init_wrapper_default_server_address(self, instrumentor):
        """_init_wrapper defaults to localhost:6379."""
        instance = Mock()
        instance.id = "worker-default"
        instance.middlewares = []

        def fake_middleware(fn):
            instance.middlewares.append(fn)
            return fn

        instance.middleware = fake_middleware

        wrapped = Mock()
        wrapped.return_value = None

        instrumentor._init_wrapper(wrapped, instance, (), {})

        assert instance._otel_server_address == "localhost"
        assert instance._otel_server_port == 6379


class TestToMs:
    """Test _to_ms static method."""

    def test_to_ms_with_none(self, instrumentor):
        assert instrumentor._to_ms(None) is None

    def test_to_ms_with_timedelta(self, instrumentor):
        assert instrumentor._to_ms(timedelta(seconds=30)) == 30000
        assert instrumentor._to_ms(timedelta(milliseconds=500)) == 500
        assert instrumentor._to_ms(timedelta(minutes=5)) == 300000

    def test_to_ms_with_int(self, instrumentor):
        assert instrumentor._to_ms(30) == 30000

    def test_to_ms_with_float(self, instrumentor):
        assert instrumentor._to_ms(1.5) == 1500
