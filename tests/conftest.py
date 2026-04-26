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

"""Pytest configuration for shared fixtures."""

from unittest.mock import Mock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry.instrumentation.streaq import StreaqInstrumentor


@pytest.fixture
def memory_exporter():
    """Create an in-memory span exporter for testing."""
    exporter = InMemorySpanExporter()
    return exporter


@pytest.fixture
def tracer_provider(memory_exporter):
    """Create a tracer provider with in-memory exporter."""
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    return provider


@pytest.fixture
def instrumentor(tracer_provider):
    """Create and instrument StreaqInstrumentor with test tracer provider."""
    instrumentor = StreaqInstrumentor()
    instrumentor.instrument(tracer_provider=tracer_provider)
    yield instrumentor
    instrumentor.uninstrument()


@pytest.fixture
def mock_worker():
    """Create a mock worker object."""
    worker = Mock()
    worker.queue_name = "test_queue"
    worker.name = "test_worker"
    worker.id = "worker-1"
    worker._redis = None
    worker.priorities = ["normal"]
    worker.concurrency = 16
    worker.sync_concurrency = 16
    worker.idle_timeout = 60000
    mock_task = Mock()
    mock_task.timeout = 30000
    worker.registry = {"test_task": mock_task}
    return worker


@pytest.fixture
def mock_task(mock_worker):
    """Create a mock task object."""
    task = Mock()
    task.id = "task-uuid-123"
    task.kwargs = {}
    task.timeout = None
    task.priority = "normal"
    task.delay = None
    task.schedule = None
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
    return task


@pytest.fixture
def mock_instance(mock_worker):
    """Create a mock task instance (Task)."""
    task = Mock()
    task.id = "task-uuid-123"
    task.kwargs = {}
    task.timeout = None
    task.priority = "normal"
    task.delay = None
    task.schedule = None
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
    return task


@pytest.fixture
def mock_msg():
    """Create a mock message object."""
    msg = Mock()
    msg.message_id = "msg-123"
    msg.task_id = "task-456"
    msg.fn_name = "test_task"
    msg.priority = "normal"
    msg.tries = 0
    msg.enqueue_time = 1718000000000
    msg.kwargs = {}
    msg.timeout = None
    msg.data = None
    return msg


@pytest.fixture
def mock_instance_with_worker():
    """Create a mock instance with worker for error handling tests."""
    worker = Mock()
    worker.queue_name = "test_queue"
    worker._redis = None
    worker.priorities = ["normal"]

    instance = Mock()
    instance.fn_name = "test_task"
    instance.worker = worker
    return instance


@pytest.fixture
def fresh_tracer_provider():
    """Create a fresh tracer provider for each test (without exporter)."""
    return TracerProvider()
