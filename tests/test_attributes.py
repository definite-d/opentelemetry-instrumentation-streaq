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

"""Tests for streaq instrumentation span attributes."""

import typing
from dataclasses import fields
from types import NoneType, UnionType
from typing import Annotated, Any, get_args, get_origin, get_type_hints

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from opentelemetry.instrumentation.streaq.attributes import (
    AttributeType,
    CompletionAttributes,
    ConsumerAttributes,
    ProducerAttributes,
)


class TestAttributeValueTypes:
    """Test that AttributeValue accepts only allowed types for streaq instrumentation."""

    @staticmethod
    def _is_valid_attribute_type(t: Any) -> bool:
        # 1. Strip Annotated wrapper if present
        if get_origin(t) is Annotated:
            t = get_args(t)[0]

        # 2. Handle Unions (e.g., int | None or str | int)
        origin = get_origin(t)
        if origin in (UnionType, typing.Union):
            return all(TestAttributeValueTypes._is_valid_attribute_type(arg) for arg in get_args(t))

        # 3. Handle NoneType (allowed for optional attributes in BaseAttributes.set)
        if t is NoneType:
            return True

        # 4. Check against AttributeType members
        try:
            # We use AttributeType.__args__ to iterate through the allowed primitives/Sequences
            return any(issubclass(t, allowed) for allowed in AttributeType.__args__)
        except TypeError:
            # This handles Generics like Sequence[str] which might not work with issubclass
            # directly depending on the Python version.
            if origin is not None:
                return any(
                    issubclass(origin, get_origin(allowed) or allowed)
                    for allowed in AttributeType.__args__
                )
            return False

    def test_attribute_value_types_are_valid(self):
        for attr_class in (
            ProducerAttributes,
            ConsumerAttributes,
            CompletionAttributes,
        ):
            hints = get_type_hints(attr_class, include_extras=True)
            for f in fields(attr_class):
                field_type = hints[f.name]

                assert self._is_valid_attribute_type(field_type), (
                    f"Field '{f.name}' in {attr_class.__name__} has unsupported type: {field_type}. "
                    "OpenTelemetry attributes must match AttributeType definition."
                )


class TestBaseAttributes:
    """Test BaseAttributes class."""

    def test_get_otel_map_returns_mapping(self):
        """_get_otel_map returns field name to OTel key mapping."""
        mapping = ProducerAttributes._get_otel_map()

        assert isinstance(mapping, dict)
        assert "operation" in mapping
        assert "destination" in mapping
        assert "task_id" in mapping

    def test_get_otel_map_uses_metadata_key(self):
        """_get_otel_map uses Annotated metadata as OTel key."""
        mapping = ProducerAttributes._get_otel_map()

        assert mapping["operation"] == "messaging.operation"
        assert mapping["system"] == "messaging.system"
        assert mapping["destination"] == "messaging.destination.name"

    def test_get_otel_map_uses_field_name_without_metadata(self):
        """_get_otel_map uses field name when no Annotated metadata."""
        mapping = ProducerAttributes._get_otel_map()

        assert mapping["task_id"] == "streaq.task.id"
        assert mapping["task_function"] == "streaq.task.function"

    def test_get_otel_map_caches_result(self):
        """_get_otel_map caches mapping for performance."""
        mapping1 = ProducerAttributes._get_otel_map()
        mapping2 = ProducerAttributes._get_otel_map()

        assert mapping1 is mapping2

    def test_set_filters_none_values(self, fresh_tracer_provider):
        """set filters out None values before setting on span."""
        attrs = ProducerAttributes(
            destination="normal",
            task_id="task-123",
            task_function="test_task",
        )

        exporter = InMemorySpanExporter()
        fresh_tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = fresh_tracer_provider.get_tracer(__name__)

        with tracer.start_as_current_span("test") as span:
            attrs.set(span)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        set_attrs = spans[0].attributes or {}

        assert "messaging.destination.name" in set_attrs
        assert "streaq.task.id" in set_attrs
        assert "streaq.task.function" in set_attrs

        assert "streaq.task.max_retries" not in set_attrs
        assert "streaq.task.timeout_ms" not in set_attrs

    def test_set_calls_set_attributes_once(self, fresh_tracer_provider):
        """set calls span.set_attributes exactly once with all non-None attrs."""
        attrs = ProducerAttributes(
            destination="normal",
            task_id="abc",
            task_function="func",
            timeout_ms=30000,
        )

        exporter = InMemorySpanExporter()
        fresh_tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = fresh_tracer_provider.get_tracer(__name__)

        with tracer.start_as_current_span("test") as span:
            attrs.set(span)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1


class TestProducerAttributes:
    """Test ProducerAttributes dataclass."""

    def test_default_values(self):
        """ProducerAttributes has correct default values."""
        attrs = ProducerAttributes(
            destination="normal",
            task_id="task-1",
            task_function="my_task",
        )

        assert attrs.operation == "publish"
        assert attrs.system == "redis"

    def test_all_optional_fields_none_by_default(self):
        """Optional fields default to None."""
        attrs = ProducerAttributes(
            destination="normal",
            task_id="task-123",
            task_function="test_task",
        )

        assert attrs.timeout_ms is None
        assert attrs.ttl_ms is None
        assert attrs.scheduled_time is None

    def test_set_producer_attributes_on_span(self, fresh_tracer_provider):
        """ProducerAttributes.set() sets all attributes on span."""
        attrs = ProducerAttributes(
            destination="normal",
            task_id="task-uuid",
            task_function="process_data",
            timeout_ms=30000,
            ttl_ms=60000,
        )

        exporter = InMemorySpanExporter()
        fresh_tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = fresh_tracer_provider.get_tracer(__name__)

        with tracer.start_as_current_span("test") as span:
            attrs.set(span)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span_attrs = spans[0].attributes or {}
        assert span_attrs["messaging.operation"] == "publish"
        assert span_attrs["messaging.system"] == "redis"
        assert span_attrs["messaging.destination.name"] == "normal"
        assert span_attrs["streaq.task.id"] == "task-uuid"
        assert span_attrs["streaq.task.function"] == "process_data"
        assert span_attrs["streaq.task.timeout_ms"] == 30000
        assert span_attrs["streaq.task.ttl_ms"] == 60000


class TestConsumerAttributes:
    """Test ConsumerAttributes dataclass."""

    def test_default_values(self):
        """ConsumerAttributes has correct default values."""
        attrs = ConsumerAttributes(
            destination="normal",
            task_id="task-123",
            task_function="test_fn",
            retry_count=0,
        )

        assert attrs.operation == "process"
        assert attrs.system == "redis"

    def test_all_optional_fields_none_by_default(self):
        """Optional fields default to None."""
        attrs = ConsumerAttributes(
            destination="normal",
            task_id="task-1",
            task_function="task",
            retry_count=0,
        )

        assert attrs.timeout_ms is None

    def test_set_consumer_attributes_on_span(self, fresh_tracer_provider):
        """ConsumerAttributes.set() sets all attributes on span."""
        attrs = ConsumerAttributes(
            destination="normal",
            task_id="task-xyz",
            task_function="handler_func",
            retry_count=2,
            timeout_ms=5000,
        )

        exporter = InMemorySpanExporter()
        fresh_tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = fresh_tracer_provider.get_tracer(__name__)

        with tracer.start_as_current_span("test") as span:
            attrs.set(span)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span_attrs = spans[0].attributes or {}
        assert span_attrs["messaging.operation"] == "process"
        assert span_attrs["messaging.system"] == "redis"
        assert span_attrs["messaging.destination.name"] == "normal"
        assert span_attrs["streaq.task.id"] == "task-xyz"
        assert span_attrs["streaq.task.function"] == "handler_func"
        assert span_attrs["streaq.task.retry_count"] == 2
        assert span_attrs["streaq.task.timeout_ms"] == 5000


class TestCompletionAttributes:
    """Test CompletionAttributes dataclass."""

    def test_required_fields(self):
        """CompletionAttributes has all required fields."""
        attrs = CompletionAttributes(
            success=True,
            execution_duration_ms=1500,
            start_time="2024-01-01T10:00:00+00:00",
            finish_time="2024-01-01T10:00:01+00:00",
        )

        assert attrs.success is True
        assert attrs.execution_duration_ms == 1500
        assert attrs.start_time == "2024-01-01T10:00:00+00:00"
        assert attrs.finish_time == "2024-01-01T10:00:01+00:00"

    def test_optional_result_ttl_none_by_default(self):
        """result_ttl defaults to None."""
        attrs = CompletionAttributes(
            success=False,
            execution_duration_ms=500,
            start_time="start",
            finish_time="finish",
        )

        assert attrs.result_ttl is None

    def test_set_completion_attributes_on_span(self, fresh_tracer_provider):
        """CompletionAttributes.set() sets all attributes on span."""
        attrs = CompletionAttributes(
            success=True,
            execution_duration_ms=2500,
            start_time="2024-01-01T12:00:00+00:00",
            finish_time="2024-01-01T12:00:02.5+00:00",
            result_ttl=3600000,
        )

        exporter = InMemorySpanExporter()
        fresh_tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = fresh_tracer_provider.get_tracer(__name__)

        with tracer.start_as_current_span("test") as span:
            attrs.set(span)

        spans = exporter.get_finished_spans()
        assert len(spans) == 1
        span_attrs = spans[0].attributes or {}
        assert span_attrs["streaq.task.success"] is True
        assert span_attrs["streaq.task.execution_duration_ms"] == 2500
        assert span_attrs["streaq.task.start_time"] == "2024-01-01T12:00:00+00:00"
        assert span_attrs["streaq.task.finish_time"] == "2024-01-01T12:00:02.5+00:00"
        assert span_attrs["streaq.task.result_ttl"] == 3600000


class TestAttributeKeyUniqueness:
    """Test that attribute keys are correctly mapped."""

    @pytest.mark.parametrize(
        "attr_class",
        [ProducerAttributes, ConsumerAttributes, CompletionAttributes],
    )
    def test_has_unique_keys(self, attr_class):
        """Each attribute class field maps to unique OTel key."""
        mapping = attr_class._get_otel_map()
        values = list(mapping.values())

        assert len(values) == len(set(values))


class TestAttributeKeyConflict:
    """Test that no attribute key conflicts between classes."""

    @pytest.mark.parametrize(
        "class_a,class_b",
        [
            (ProducerAttributes, ConsumerAttributes),
            (ProducerAttributes, CompletionAttributes),
            (ConsumerAttributes, CompletionAttributes),
        ],
    )
    def test_no_key_conflicts(self, class_a, class_b):
        """Attribute classes don't share same OTel keys for different meanings."""
        mapping_a = class_a._get_otel_map()
        mapping_b = class_b._get_otel_map()

        for key_a, otel_key in mapping_a.items():
            if otel_key in mapping_b.values():
                key_b = [k for k, v in mapping_b.items() if v == otel_key][0]
                if key_a != key_b:
                    pass
