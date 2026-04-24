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

"""Test tasks for streaq instrumentation integration tests.

This file defines mock task functions that simulate streaq tasks for testing purposes.
Since the actual streaq library may not be available or have a different API,
we use simple functions that can be mocked in tests.
"""


class CustomError(Exception):
    """Custom exception for testing error handling."""

    pass


def task_add(num_a: int, num_b: int) -> int:
    """Simple addition task."""
    return num_a + num_b


def task_multiply(num_a: int, num_b: int) -> int:
    """Simple multiplication task."""
    return num_a * num_b


def task_raises() -> None:
    """Task that raises a custom error."""
    raise CustomError("The task failed!")


async def async_task_add(num_a: int, num_b: int) -> int:
    """Async addition task."""
    return num_a + num_b
