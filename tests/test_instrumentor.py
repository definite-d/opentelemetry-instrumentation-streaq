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
