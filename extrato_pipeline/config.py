import os
from pathlib import Path

ENV = os.environ.get("ENV", "local")

OWNERS = [
    o.strip() for o in os.environ.get("OWNERS", "person_1").split(",") if o.strip()
]

RAW_DATA_DIR = Path(
    os.environ.get("RAW_DATA_DIR", Path(__file__).parent.parent / "data")
)
LOCAL_STORAGE_DIR = Path(
    os.environ.get(
        "LOCAL_STORAGE_DIR",
        Path(os.environ.get("DAGSTER_HOME", str(RAW_DATA_DIR))) / "local_data",
    )
)
