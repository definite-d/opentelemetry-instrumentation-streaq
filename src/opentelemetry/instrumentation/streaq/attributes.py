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

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields
from typing import Annotated, Any, ClassVar, get_type_hints

from opentelemetry.trace import Span

AttributeValue = (
    str
    | int
    | float
    | bool
    | Sequence[str]
    | Sequence[int]
    | Sequence[float]
    | Sequence[bool]
)


@dataclass(kw_only=True)
class BaseAttributes:
    _key_cache: ClassVar[dict[type["BaseAttributes"], dict[str, str]]] = {}

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
        otel_map: dict[str, str] = self._get_otel_map()
        attrs: dict[str, AttributeValue] = {}

        for f in fields(self):
            val: Any = getattr(self, f.name)
            if val is not None:
                attrs[otel_map[f.name]] = val

        if attrs:
            span.set_attributes(attrs)


@dataclass(kw_only=True)
class ProducerAttributes(BaseAttributes):
    operation: Annotated[str, "messaging.operation"] = "publish"
    system: Annotated[str, "messaging.system"] = "redis"
    destination: Annotated[str, "messaging.destination"]

    task_id: Annotated[str, "streaq.task.id"]
    task_function: Annotated[str, "streaq.task.function"]
    task_priority: Annotated[str, "streaq.task.priority"]

    max_retries: Annotated[int | None, "streaq.task.max_retries"] = None
    timeout_ms: Annotated[int | None, "streaq.task.timeout_ms"] = None
    ttl_ms: Annotated[int | None, "streaq.task.ttl_ms"] = None
    delay_ms: Annotated[int | None, "streaq.task.delay_ms"] = None
    expire_ms: Annotated[int | None, "streaq.task.expire_ms"] = None
    unique: Annotated[bool | None, "streaq.task.unique"] = None
    dependencies: Annotated[Sequence[str] | None, "streaq.task.dependencies"] = None
    crontab: Annotated[str | None, "streaq.task.crontab"] = None
    scheduled_time: Annotated[str | None, "streaq.task.scheduled_time"] = None


@dataclass(kw_only=True)
class ConsumerAttributes(BaseAttributes):
    operation: Annotated[str, "messaging.operation"] = "process"
    system: Annotated[str, "messaging.system"] = "redis"
    destination: Annotated[str, "messaging.destination"]
    message_id: Annotated[str, "messaging.message.id"]

    client_id: Annotated[str, "messaging.client.id"]
    consumer_id: Annotated[str, "messaging.consumer.id"]
    worker_concurrency: Annotated[int, "streaq.worker.concurrency"]
    worker_priorities: Annotated[str, "streaq.worker.priorities"]

    task_id: Annotated[str, "streaq.task.id"]
    task_function: Annotated[str, "streaq.task.function"]
    task_priority: Annotated[str, "streaq.task.priority"]
    retry_count: Annotated[int, "streaq.task.retry_count"]

    enqueue_time: Annotated[str, "streaq.task.enqueue_time"]

    timeout_ms: Annotated[int | None, "streaq.task.timeout_ms"] = None
    worker_sync_concurrency: Annotated[int | None, "streaq.worker.sync_concurrency"] = (
        None
    )


@dataclass(kw_only=True)
class CompletionAttributes(BaseAttributes):
    success: Annotated[bool, "streaq.task.success"]
    execution_duration_ms: Annotated[int, "streaq.task.execution_duration_ms"]

    start_time: Annotated[str, "streaq.task.start_time"]
    finish_time: Annotated[str, "streaq.task.finish_time"]
    enqueue_time: Annotated[str, "streaq.task.enqueue_time"]

    result_ttl: Annotated[int | None, "streaq.task.result_ttl"] = None
