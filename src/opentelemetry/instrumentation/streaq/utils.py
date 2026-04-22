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

"""Utility functions for streaQ OpenTelemetry instrumentation."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Optional

from opentelemetry.trace import Span

logger = logging.getLogger(__name__)

# Key used to store/retrieve trace context metadata in task kwargs
OTEL_METADATA_KEY = "_streaq_otel_metadata"


class SpanAttributes(str, Enum):
    """OpenTelemetry span attribute names for streaQ instrumentation."""

    # Messaging system semantic conventions
    MESSAGING_SYSTEM = "messaging.system"
    MESSAGING_DESTINATION_NAME = "messaging.destination.name"
    MESSAGING_MESSAGE_ID = "messaging.message.id"
    MESSAGING_OPERATION = "messaging.operation"

    # streaQ-specific attributes
    STREAQ_TASK_NAME = "messaging.streaq.task_name"
    STREAQ_RETRY_COUNT = "messaging.streaq.retry_count"
    STREAQ_WORKER_NAME = "messaging.streaq.worker_name"


def inject_metadata(task_kwargs: dict[str, Any], metadata: dict[str, str]) -> None:
    """Inject trace context metadata into task kwargs.

    This is the producer-side function that adds trace context to the
    task metadata that will be propagated to the worker.

    Args:
        task_kwargs: The kwargs dict for the task (from Task.enqueue)
        metadata: Dictionary containing traceparent, tracestate, etc.
    """
    task_kwargs.setdefault(OTEL_METADATA_KEY, {})
    task_kwargs[OTEL_METADATA_KEY].update(metadata)

def set_span_attributes_from_task(
    span: Span,
    task_name: str,
    task_id: str,
    queue_name: str,
    worker_name: Optional[str] = None,
    retry_count: Optional[int] = None,
) -> None:
    """Set OpenTelemetry semantic convention attributes on a span.

    Args:
        span: The span to set attributes on
        task_name: The name of the task function
        task_id: The unique streaQ task identifier
        queue_name: The stream/queue name
        worker_name: Optional worker identifier
        retry_count: Optional current retry attempt number
    """
    if not span.is_recording():
        return

    # Messaging semantic conventions
    span.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "redis")
    span.set_attribute(SpanAttributes.MESSAGING_DESTINATION_NAME, queue_name)
    span.set_attribute(SpanAttributes.MESSAGING_MESSAGE_ID, task_id)
    span.set_attribute(SpanAttributes.STREAQ_TASK_NAME, task_name)

    if worker_name is not None:
        span.set_attribute(SpanAttributes.STREAQ_WORKER_NAME, worker_name)
    if retry_count is not None:
        span.set_attribute(SpanAttributes.STREAQ_RETRY_COUNT, retry_count)
