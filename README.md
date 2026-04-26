# OpenTelemetry streaQ Instrumentation

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Versions](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)

OpenTelemetry instrumentation for [streaQ](https://github.com/tastyware/streaq), a fast, async, type-safe Python job queue using Redis Streams.

> **Disclaimer:** This is a third-party package maintained independently and is not part of the official [OpenTelemetry Python Contrib](https://github.com/open-telemetry/opentelemetry-python-contrib) repository. However, it was derived from the [`opentelemetry-instrumentation-celery`](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-celery) package.
>
> As it is a third-party package, the `opentelemetry-bootstrap -a install` command will **not** find and install this package for streaQ projects. It _must_ be manually added to your project dependencies, before you can then use `opentelemetry-instrument`.

## Features

- **Distributed Tracing**: Automatically creates producer and consumer spans for task queue operations
- **Context Propagation**: Seamlessly propagates trace context from task producers to workers
- **Semantic Conventions**: Follows OpenTelemetry messaging semantic conventions
- **Redis Integration**: Captures Redis connection attributes and messaging system details
- **Error Tracking**: Records exceptions and task failures with full stack traces

## Installation

```bash
pip install opentelemetry-instrumentation-streaq
```

Or with the optional instruments dependency:

```bash
pip install opentelemetry-instrumentation-streaq[instruments]
```

### Requirements

- Python 3.10+
- streaq >= 6.4.0, < 7.0
- OpenTelemetry API ~= 1.12

## Usage

### Quick Start

Create a worker module (worker.py):

```python
from streaq import Worker
from opentelemetry.instrumentation.streaq import StreaqInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Instrument streaQ
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
StreaqInstrumentor().instrument(tracer_provider=tracer_provider)

worker = Worker(redis_url="redis://localhost")

@worker.task
async def my_task(data: str) -> str:
    return f"Processed: {data}"
```

Run the worker:

```bash
streaq run worker:worker
```

Queue a task (script.py):

```python
from anyio import run
from worker import worker, my_task

async def main():
    async with worker:
        await my_task.enqueue("hello")

run(main)
```

### With Custom Tracer Provider

```python
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.instrumentation.streaq import StreaqInstrumentor

provider = TracerProvider()
StreaqInstrumentor().instrument(tracer_provider=provider)
```

### Combining with Manual Instrumentation

```python
from opentelemetry import trace

@worker.task
async def my_task(data: str) -> str:
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("business_logic"):
        return f"Processed: {data}"
```

### Disabling Instrumentation

```python
from opentelemetry.instrumentation.utils import disable_instrumentation

disable_instrumentation("streaq")
```

## How It Works

The instrumentation patches streaQ at two key points:

1. **Producer Side** (`Task._enqueue`): Creates `PRODUCER` spans when tasks are enqueued and injects trace context into task metadata.
2. **Consumer Side** (`Worker.run_task`): Extracts trace context and creates `CONSUMER` spans when tasks are processed by workers.

This ensures complete end-to-end tracing across the distributed task queue.

## Span Attributes

### Producer Span Attributes

These attributes are captured when tasks are enqueued:

| Attribute                    | Type     | Description                            |
| ---------------------------- | -------- | -------------------------------------- |
| `messaging.system`           | string   | Always `"redis"`                       |
| `messaging.operation`        | string   | Always `"publish"`                     |
| `messaging.destination.name` | string   | The queue name (e.g., `"normal"`)      |
| `streaq.task.id`             | string   | Unique task identifier                 |
| `streaq.task.function`       | string   | Name of the task function              |
| `streaq.task.max_retries`    | int      | Maximum retry attempts (if configured) |
| `streaq.task.timeout_ms`     | int      | Task timeout in milliseconds           |
| `streaq.task.ttl_ms`         | int      | Task TTL in milliseconds               |
| `streaq.task.delay_ms`       | int      | Task delay in milliseconds             |
| `streaq.task.expire_ms`      | int      | Task expiration in milliseconds        |
| `streaq.task.unique`         | boolean  | Whether task is unique                 |
| `streaq.task.dependencies`   | string[] | Task dependencies (after)              |
| `streaq.task.crontab`        | string   | Crontab schedule (if scheduled)        |
| `streaq.task.scheduled_time` | string   | Scheduled execution time (if delayed)  |

### Consumer Span Attributes

These attributes are captured when tasks are executed:

| Attribute                        | Type   | Description                              |
| -------------------------------- | ------ | ---------------------------------------- |
| `messaging.system`               | string | Always `"redis"`                         |
| `messaging.operation`            | string | Always `"process"`                       |
| `messaging.destination.name`     | string | The queue name                           |
| `messaging.message.id`           | string | The message identifier                   |
| `messaging.consumer.id`          | string | Worker consumer identifier               |
| `streaq.worker.concurrency`      | int    | Worker concurrency setting               |
| `streaq.worker.priorities`       | string | Worker priority levels (comma-separated) |
| `streaq.task.id`                 | string | Unique task identifier                   |
| `streaq.task.function`           | string | Name of the task function                |
| `streaq.task.retry_count`        | int    | Current retry attempt                    |
| `streaq.task.enqueue_time`       | string | When the task was enqueued (ISO format)  |
| `streaq.task.timeout_ms`         | int    | Task timeout in milliseconds             |
| `streaq.worker.sync_concurrency` | int    | Synchronous task concurrency             |

### Completion Attributes

These attributes are added to consumer spans after task execution:

| Attribute                           | Type    | Description                             |
| ----------------------------------- | ------- | --------------------------------------- |
| `streaq.task.success`               | boolean | Whether task execution succeeded        |
| `streaq.task.execution_duration_ms` | int     | Task execution duration in milliseconds |
| `streaq.task.start_time`            | string  | Task start time (ISO format)            |
| `streaq.task.finish_time`           | string  | Task finish time (ISO format)           |
| `streaq.task.result_ttl`            | int     | Result TTL in milliseconds              |

## API Reference

### StreaqInstrumentor

```python
class StreaqInstrumentor(BaseInstrumentor)
```

The main instrumentor class for streaQ.

#### Methods

##### `instrument(**kwargs)`

Enable streaQ instrumentation.

**Parameters:**

- `tracer_provider` (optional): Custom tracer provider. If not provided, uses the global provider.
- `meter_provider` (optional): Custom meter provider for metrics (future).

##### `uninstrument()`

Disable streaQ instrumentation and remove all patches.

#### Example

```python
from opentelemetry.instrumentation.streaq import StreaqInstrumentor

# Enable instrumentation
StreaqInstrumentor().instrument()

# ... your streaQ code ...

# Disable instrumentation
StreaqInstrumentor().uninstrument()
```

### Utilities

#### `inject_metadata(task_kwargs, metadata)`

Inject trace context metadata into task kwargs.

```python
from opentelemetry.instrumentation.streaq.utils import inject_metadata

inject_metadata(task_kwargs, {"traceparent": "00-...", "tracestate": "..."})
```

#### `extract_metadata(task_kwargs)`

Extract trace context metadata from task kwargs.

```python
from opentelemetry.instrumentation.streaq.utils import extract_metadata

metadata = extract_metadata(task_kwargs)
# Returns: {"traceparent": "00-...", "tracestate": "..."}
```

#### `OTEL_METADATA_KEY`

The key used to store trace context in task kwargs (`"__otel_metadata"`).

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## References

- [streaQ on GitHub](https://github.com/tastyware/streaq)
- [OpenTelemetry Python Documentation](https://opentelemetry-python.readthedocs.io/)
- [OpenTelemetry Project](https://opentelemetry.io/)
- [OpenTelemetry Python Examples](https://github.com/open-telemetry/opentelemetry-python/tree/main/docs/examples)
