import itertools
from pathlib import Path

import pytest

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta

EXAMPLES = Path(__file__).parent.parent / "examples" / "contracts"
_ENV_NUMBERS = itertools.count(1)


@pytest.fixture
def examples_dir() -> Path:
    return EXAMPLES


@pytest.fixture(scope="session")
def compute(tmp_path_factory: pytest.TempPathFactory) -> LocalSparkDelta:
    """One local Spark + Delta compute for the whole test session."""
    return LocalSparkDelta(tmp_path_factory.mktemp("contractline"))


@pytest.fixture
def env() -> str:
    """A fresh ephemeral environment name per test, so tables never collide."""
    return f"pr{next(_ENV_NUMBERS)}"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Tests that start Spark are marked ``spark`` so CI can run them once."""
    for item in items:
        if "compute" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)
