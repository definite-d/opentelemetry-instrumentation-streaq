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
Utility functions for streaQ trace context propagation.

Provides :class:`StreaqMetadataGetter` for extracting trace context from
task kwargs, and :func:`inject_metadata`/:func:`extract_metadata` for
context propagation between producers and consumers.
"""

from __future__ import annotations

import logging
from typing import Any

from opentelemetry.propagators.textmap import Getter

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
    """Extract trace context metadata from task kwargs.

    This is the consumer-side function that retrieves and removes the trace
    context from task kwargs that was previously injected by the producer.

    The metadata is removed from kwargs after extraction to avoid carrying
    it forward through subsequent operations.

    Args:
        task_kwargs: The kwargs dict from the task message, containing the
            injected ``__otel_metadata``.

    Returns:
        Dictionary containing trace context fields (traceparent, tracestate),
        or an empty dict if no metadata was found.

    Example:
        See module-level example in ``inject_metadata``.
    """
    metadata = task_kwargs.pop(OTEL_METADATA_KEY, None)
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        logger.debug("Invalid metadata type: %s", type(metadata))
        return {}
    return metadata


class StreaqMetadataGetter(Getter[dict[str, str]]):
    """Custom text map getter for streaQ task kwargs."""

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
