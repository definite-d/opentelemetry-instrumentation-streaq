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
Span attribute definitions for streaQ instrumentation.

Uses Python's ``Annotated`` type to associate field names with OpenTelemetry
attribute keys. See :class:`ProducerAttributes`, :class:`ConsumerAttributes`,
and :class:`CompletionAttributes` for available attributes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields
from typing import Annotated, Any, ClassVar, get_type_hints

from opentelemetry.trace import Span

AttributeType = (
    str | int | float | bool | Sequence[str] | Sequence[int] | Sequence[float] | Sequence[bool]
)


@dataclass(kw_only=True)
class BaseAttributes:
    """Base class for span attributes."""

    _pairs_cache: ClassVar[dict[type[BaseAttributes], list[tuple[str, str]]]] = {}

    @classmethod
    def _get_otel_pairs(cls) -> list[tuple[str, str]]:
        if cls not in cls._pairs_cache:
            hints: dict[str, Any] = get_type_hints(cls, include_extras=True)
            pairs: list[tuple[str, str]] = []
            for f in fields(cls):
                hint = hints.get(f.name)
                if hasattr(hint, "__metadata__") and hint.__metadata__:
                    pairs.append((f.name, str(hint.__metadata__[0])))
                else:
                    pairs.append((f.name, f.name))
            cls._pairs_cache[cls] = pairs
        return cls._pairs_cache[cls]

    def set(self, span: Span) -> None:
        """Set non-None attributes on span."""
        attrs: dict[str, AttributeType] = {}

        for field_name, otel_key in self._get_otel_pairs():
            val: Any = getattr(self, field_name)
            if val is not None:
                attrs[otel_key] = val

        if attrs:
            span.set_attributes(attrs)


@dataclass(kw_only=True)
class ProducerAttributes(BaseAttributes):
    """Producer span attributes for tasks enqueued via ``Task.enqueue()``."""

    operation_type: Annotated[str, "messaging.operation.type"] = "send"
    """The task enqueue operation. Always ``"send"``."""

    system: Annotated[str, "messaging.system"] = "redis"
    """The messaging system backing streaQ. Currently always ``"redis"``."""

    destination: Annotated[str, "messaging.destination.name"]
    """The destination queue name. Maps from streaQ's priority queue parameter."""

    message_id: Annotated[str, "messaging.message.id"]
    """Unique message identifier. Maps from ``streaq.task.id``."""

    operation_name: Annotated[str, "messaging.operation.name"]
    """The task function name. Maps from ``streaq.task.function``."""

    timeout_ms: Annotated[int | None, "streaq.task.timeout_ms"] = None
    """Task timeout in milliseconds."""

    ttl_ms: Annotated[int | None, "streaq.task.ttl_ms"] = None
    """Result TTL in milliseconds."""

    max_retries: Annotated[int | None, "streaq.task.max_retries"] = None
    """Maximum retry attempts. Maps from ``RegisteredTask.max_tries``."""

    delay_ms: Annotated[int | None, "streaq.task.delay_ms"] = None
    """Task delay in milliseconds."""

    expire_ms: Annotated[int | None, "streaq.task.expire_ms"] = None
    """Task expiration in milliseconds. Maps from ``RegisteredTask.expire``."""

    unique: Annotated[bool | None, "streaq.task.unique"] = None
    """Whether the task is unique. Maps from ``RegisteredTask.unique``."""

    dependencies: Annotated[list[str] | None, "streaq.task.dependencies"] = None
    """Dependency task IDs. Maps from ``task.after``."""

    crontab: Annotated[str | None, "streaq.task.crontab"] = None
    """Crontab schedule expression. Maps from ``task.schedule`` when it is a string."""

    scheduled_time: Annotated[str | None, "streaq.task.scheduled_time"] = None
    """Scheduled execution time as ISO-8601 string."""


@dataclass(kw_only=True)
class ConsumerAttributes(BaseAttributes):
    """Consumer span attributes for tasks executed by workers.

    Note: Task ID, function, and timeout are inherited from parent via
    trace context propagation. Only operation, system, retry_count
    need to be set by the consumer middleware.
    """

    operation_type: Annotated[str, "messaging.operation.type"] = "process"
    """The task process operation. Always ``"process"``."""

    system: Annotated[str, "messaging.system"] = "redis"
    """The messaging system backing streaQ. Currently always ``"redis"``."""

    destination: Annotated[str, "messaging.destination.name"]
    """The destination queue name. Maps from streaQ's priority queue parameter."""

    message_id: Annotated[str, "messaging.message.id"]
    """Unique message identifier. Maps from ``streaq.task.id``, inherited from the producer span."""

    operation_name: Annotated[str, "messaging.operation.name"]
    """The task function name. Maps from ``streaq.task.function``, inherited from the producer span."""

    consumer_id: Annotated[str, "messaging.consumer.id"]
    """The consumer (worker) identifier. Maps from ``Worker.id``."""

    retry_count: Annotated[int, "streaq.task.retry_count"]
    """Current retry attempt for this task. Maps from ``TaskContext.tries``."""

    timeout_ms: Annotated[int | None, "streaq.task.timeout_ms"] = None
    """Task timeout in milliseconds. Inherited from the producer span."""


@dataclass(kw_only=True)
class CompletionAttributes(BaseAttributes):
    """Completion attributes added to consumer spans after task execution."""

    success: Annotated[bool, "streaq.task.success"]
    """Whether the task completed successfully."""

    execution_duration_ms: Annotated[int, "streaq.task.execution_duration_ms"]
    """Task execution wall-clock duration in milliseconds."""

    start_time: Annotated[str | None, "streaq.task.start_time"] = None
    """ISO-8601 timestamp when task execution started."""

    finish_time: Annotated[str | None, "streaq.task.finish_time"] = None
    """ISO-8601 timestamp when task execution finished."""

    result_ttl: Annotated[int | None, "streaq.task.result_ttl"] = None
    """Result storage TTL in milliseconds."""
