import importlib.util
import os
import sys
import uuid
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = REPO_ROOT / "fabric" / "notebooks" / "nb_sales_pipeline.Notebook" / "notebook-content.py"
SAMPLE_CSV = REPO_ROOT / "data" / "sample" / "sales.csv"


@pytest.fixture(scope="session")
def pipeline():
    """Import the Fabric notebook as a module; its run cell is guarded by __name__."""
    spec = importlib.util.spec_from_file_location("nb_sales_pipeline", NOTEBOOK_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    # Spark workers must use the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("sales-pipeline-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def tables():
    """Unique table names per test so tests stay independent within one Spark session."""
    suffix = uuid.uuid4().hex[:8]
    return {
        "bronze": f"raw_sales_{suffix}",
        "silver": f"sales_clean_{suffix}",
        "quarantine": f"sales_quarantine_{suffix}",
        "gold": f"daily_sales_summary_{suffix}",
    }


@pytest.fixture
def sample_csv():
    return str(SAMPLE_CSV)
