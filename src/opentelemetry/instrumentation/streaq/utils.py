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

import logging
from enum import Enum
from typing import Any

from opentelemetry.propagators.textmap import Getter
from opentelemetry.trace import Span

logger = logging.getLogger(__name__)

# Key used to store/retrieve trace context metadata in task kwargs
OTEL_METADATA_KEY = "__otel_metadata"


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


def extract_metadata(task_kwargs: dict[str, Any]) -> dict[str, str]:
    metadata = task_kwargs.pop(OTEL_METADATA_KEY, None)
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        logger.debug("Invalid metadata type: %s", type(metadata))
        return {}
    return metadata


class StreaqMetadataGetter(Getter[dict[str, str]]):
    _instance: StreaqMetadataGetter | None = None

    def __new__(cls) -> StreaqMetadataGetter:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get(self, carrier: dict[str, str] | None, key: str) -> list[str] | None:
        if carrier is None or not isinstance(carrier, dict):
            return None
        value = carrier.get(key)
        if value is None:
            return None
        return [str(value)]

    def keys(self, carrier: dict[str, str] | None) -> list[str]:
        if carrier is None or not isinstance(carrier, dict):
            return []
        return list(carrier.keys())
