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

    _key_cache: ClassVar[dict[type[BaseAttributes], dict[str, str]]] = {}

    @classmethod
    def _get_otel_map(cls) -> dict[str, str]:
        if cls not in cls._key_cache:
            hints: dict[str, Any] = get_type_hints(cls, include_extras=True)
            mapping: dict[str, str] = {}
            for f in fields(cls):
                hint = hints.get(f.name)
                if hasattr(hint, "__metadata__") and hint.__metadata__:
                    mapping[f.name] = str(hint.__metadata__[0])
                else:
                    mapping[f.name] = f.name
            cls._key_cache[cls] = mapping
        return cls._key_cache[cls]

    def set(self, span: Span) -> None:
        """Set non-None attributes on span."""
        otel_map: dict[str, str] = self._get_otel_map()
        attrs: dict[str, AttributeType] = {}

        for f in fields(self):
            val: Any = getattr(self, f.name)
            if val is not None:
                attrs[otel_map[f.name]] = val

        if attrs:
            span.set_attributes(attrs)


@dataclass(kw_only=True)
class ProducerAttributes(BaseAttributes):
    """Producer span attributes for tasks enqueued via ``Task.enqueue()``."""

    operation: Annotated[str, "messaging.operation"] = "publish"
    """Always ``"publish"``."""

    system: Annotated[str, "messaging.system"] = "redis"
    """Messaging system backend (currently always redis)."""

    destination: Annotated[str, "messaging.destination.name"]
    """Queue name (in streaQ, priority is the queue name)."""

    task_id: Annotated[str, "streaq.task.id"]
    """Unique task identifier."""

    task_function: Annotated[str, "streaq.task.function"]
    """Task function name."""

    max_retries: Annotated[int | None, "streaq.task.max_retries"] = None
    """Max retry attempts."""

    timeout_ms: Annotated[int | None, "streaq.task.timeout_ms"] = None
    """Timeout in milliseconds."""

    ttl_ms: Annotated[int | None, "streaq.task.ttl_ms"] = None
    """TTL in milliseconds."""

    delay_ms: Annotated[int | None, "streaq.task.delay_ms"] = None
    """Delay in milliseconds."""

    expire_ms: Annotated[int | None, "streaq.task.expire_ms"] = None
    """Expiration in milliseconds."""

    unique: Annotated[bool | None, "streaq.task.unique"] = None
    """Whether task is unique."""

    dependencies: Annotated[Sequence[str] | None, "streaq.task.dependencies"] = None
    """Task dependencies."""

    crontab: Annotated[str | None, "streaq.task.crontab"] = None
    """Crontab schedule."""

    scheduled_time: Annotated[str | None, "streaq.task.scheduled_time"] = None
    """Scheduled execution time."""


@dataclass(kw_only=True)
class ConsumerAttributes(BaseAttributes):
    """Consumer span attributes for tasks executed by workers."""

    operation: Annotated[str, "messaging.operation"] = "process"
    """Always ``"process"``."""

    system: Annotated[str, "messaging.system"] = "redis"
    """Messaging system backend (currently always redis)."""

    destination: Annotated[str, "messaging.destination.name"]
    """Queue name (in streaQ, priority is the queue name)."""

    message_id: Annotated[str, "messaging.message.id"]
    """Message identifier."""

    consumer_id: Annotated[str, "messaging.consumer.id"]
    """Worker consumer ID."""

    worker_concurrency: Annotated[int, "streaq.worker.concurrency"]
    """Worker concurrency."""

    worker_priorities: Annotated[str, "streaq.worker.priorities"]
    """Worker priorities (comma-separated list of available queue names)."""

    task_id: Annotated[str, "streaq.task.id"]
    """Task identifier."""

    task_function: Annotated[str, "streaq.task.function"]
    """Task function name."""

    retry_count: Annotated[int, "streaq.task.retry_count"]
    """Retry attempt number."""

    enqueue_time: Annotated[str | None, "streaq.task.enqueue_time"] = None
    """Enqueue timestamp."""

    timeout_ms: Annotated[int | None, "streaq.task.timeout_ms"] = None
    """Timeout in milliseconds."""

    worker_sync_concurrency: Annotated[int | None, "streaq.worker.sync_concurrency"] = None
    """Sync concurrency."""


@dataclass(kw_only=True)
class CompletionAttributes(BaseAttributes):
    """Completion attributes added to consumer spans after task execution."""

    success: Annotated[bool, "streaq.task.success"]
    """ Whether task succeeded."""

    execution_duration_ms: Annotated[int, "streaq.task.execution_duration_ms"]
    """Duration in milliseconds."""

    start_time: Annotated[str | None, "streaq.task.start_time"] = None
    """Start timestamp."""

    finish_time: Annotated[str | None, "streaq.task.finish_time"] = None
    """Finish timestamp."""

    result_ttl: Annotated[int | None, "streaq.task.result_ttl"] = None
    """Result TTL."""
