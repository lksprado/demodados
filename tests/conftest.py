import os
import sys
from pathlib import Path


def pytest_sessionstart(session):
    # Ensure local_setup/src is importable in tests
    here = Path(__file__).resolve()
    src = here.parents[1] / "src"
    sys.path.insert(0, str(src))
