# OpenTelemetry Streaq Instrumentation (in progress)

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Versions](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)

OpenTelemetry instrumentation for [streaQ](https://github.com/tastyware/streaq), a fast, async, type-safe Python job queue using Redis Streams.

> **Disclaimer:** This is a third-party package maintained independently and is not part of the official [OpenTelemetry Python Contrib](https://github.com/open-telemetry/opentelemetry-python-contrib) repository. However, it was derived from the [`opentelemetry-instrumentation-celery`](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-celery) package.
>
> As it is a third-party package, the `opentelemetry-bootstrap -a install` command will **not** find and install this package for streaQ projects. It must be manually added to your project dependencies, before you can then use `opentelemetry-instrument`.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
