from pathlib import Path
import os


PROJECT_ROOT = Path(__file__).resolve().parents[4]

WAREHOUSE_PATH = Path(
    os.getenv(
        "WAREHOUSE_PATH",
        PROJECT_ROOT / "phase3" / "de6" / "warehouse" / "network_analytics.db"
    )
)

AS_OF = os.getenv("AS_OF")