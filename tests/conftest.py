from pathlib import Path

import pytest

EXAMPLES = Path(__file__).parent.parent / "examples" / "contracts"


@pytest.fixture
def examples_dir() -> Path:
    return EXAMPLES
