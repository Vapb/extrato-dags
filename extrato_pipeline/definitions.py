from dagster import Definitions
from dagster import load_assets_from_modules

from extrato_pipeline.assets import bronze
from extrato_pipeline.assets import markdown
from extrato_pipeline.config import ENV
from extrato_pipeline.config import LOCAL_STORAGE_DIR
from extrato_pipeline.config import RAW_DATA_DIR
from extrato_pipeline.io_managers import TextIOManager
from extrato_pipeline.resources import RawDataSource

TEXT_IO_MANAGERS = {
    "local": TextIOManager(data_dir=str(LOCAL_STORAGE_DIR)),
    # TODO: "prod": S3TextIOManager(bucket=...) — write the markdown to S3
}

RAW_DATA_SOURCES = {
    "local": RawDataSource(data_dir=str(RAW_DATA_DIR)),
    # TODO: "prod": S3RawDataSource(bucket=...) — read the raw PDFs from S3
}

defs = Definitions(
    assets=load_assets_from_modules([bronze, markdown]),
    resources={
        "text_io_manager": TEXT_IO_MANAGERS[ENV],
        "raw_data_source": RAW_DATA_SOURCES[ENV],
    },
)
