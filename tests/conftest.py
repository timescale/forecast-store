"""Load repo-root .env so live tests pick up FORECAST_STORE_TEST_DSN automatically.

Real environment variables win over .env values; .env is gitignored
(credentials never enter history) — see .env.example.
"""

import os
from pathlib import Path


def _load_dotenv() -> None:
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_dotenv()
