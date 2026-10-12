"""The local half of the parity test, run in the normal dev environment."""

import json
import sys
import tempfile
from pathlib import Path

from contractline.adapters.compute.spark_delta_local import LocalSparkDelta
from tests_databricks.test_parity import run_everywhere

if __name__ == "__main__":
    tables = run_everywhere(LocalSparkDelta(Path(tempfile.mkdtemp())))
    Path(sys.argv[1]).write_text(json.dumps(tables))
