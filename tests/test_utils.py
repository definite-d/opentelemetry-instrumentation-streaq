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

"""Tests for streaq instrumentation utilities."""

from datetime import datetime, timedelta

import pytest
from opentelemetry.trace import Span

from opentelemetry.instrumentation.streaq.utils import (
    OTEL_METADATA_KEY,
    StreaqMetadataGetter,
    extract_metadata,
    inject_metadata,
)


class TestStreaqMetadataGetter:
    """Test the singleton StreaqMetadataGetter."""

    def test_singleton_pattern(self):
        """Getter should be a singleton."""
        getter1 = StreaqMetadataGetter()
        getter2 = StreaqMetadataGetter()
        assert getter1 is getter2

    def test_get_from_dict(self):
        """Get values from dictionary carrier."""
        getter = StreaqMetadataGetter()
        carrier = {"traceparent": "tp-value", "tracestate": "ts-value"}

        assert getter.get(carrier, "traceparent") == ["tp-value"]
        assert getter.get(carrier, "tracestate") == ["ts-value"]
        assert getter.get(carrier, "missing") is None

    def test_get_from_none(self):
        """Handle None carrier gracefully."""
        getter = StreaqMetadataGetter()
        assert getter.get(None, "traceparent") is None

    def test_get_from_non_dict(self):
        """Handle non-dict carrier gracefully."""
        getter = StreaqMetadataGetter()
        assert getter.get("not a dict", "key") is None
        assert getter.get([1, 2, 3], "key") is None

    def test_keys_from_dict(self):
        """Get all keys from dictionary carrier."""
        getter = StreaqMetadataGetter()
        carrier = {"key1": "val1", "key2": "val2"}
        assert getter.keys(carrier) == ["key1", "key2"]

    def test_keys_from_none(self):
        """Handle None carrier in keys()."""
        getter = StreaqMetadataGetter()
        assert getter.keys(None) == []


@pytest.mark.utils
class TestMetadataFunctions:
    """Test inject_metadata and extract_metadata."""

    def test_inject_metadata_creates_key(self):
        """inject_metadata should create OTEL metadata key."""
        kwargs = {}
        carrier = {"traceparent": "tp-123", "tracestate": "ts-abc"}

        inject_metadata(kwargs, carrier)

        assert OTEL_METADATA_KEY in kwargs
        assert kwargs[OTEL_METADATA_KEY]["traceparent"] == "tp-123"
        assert kwargs[OTEL_METADATA_KEY]["tracestate"] == "ts-abc"

    def test_inject_metadata_preserves_existing(self):
        """inject_metadata should preserve existing kwargs."""
        kwargs = {"existing": "value", "data": 123}
        carrier = {"traceparent": "tp-456"}

        inject_metadata(kwargs, carrier)

        assert kwargs["existing"] == "value"
        assert kwargs["data"] == 123
        assert OTEL_METADATA_KEY in kwargs

    def test_extract_metadata_found(self):
        """extract_metadata should return metadata when present."""
        kwargs = {
            OTEL_METADATA_KEY: {"traceparent": "tp-789", "tracestate": "ts-xyz"},
            "other": "data",
        }

        metadata = extract_metadata(kwargs)

        assert metadata == {"traceparent": "tp-789", "tracestate": "ts-xyz"}

    def test_extract_metadata_not_found(self):
        """extract_metadata should return empty dict when not present."""
        kwargs = {"other": "data"}

        metadata = extract_metadata(kwargs)

        assert metadata == {}

    def test_extract_metadata_removes_key(self):
        """extract_metadata should remove OTEL key from kwargs."""
        kwargs = {
            OTEL_METADATA_KEY: {"traceparent": "tp"},
            "other": "data",
        }

        extract_metadata(kwargs)

        assert OTEL_METADATA_KEY not in kwargs

    def test_extract_metadata_invalid_type(self):
        """extract_metadata should handle non-dict metadata."""
        kwargs = {OTEL_METADATA_KEY: "not a dict"}

        metadata = extract_metadata(kwargs)

        assert metadata == {}
        assert OTEL_METADATA_KEY not in kwargs
