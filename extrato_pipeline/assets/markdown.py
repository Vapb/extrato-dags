import pymupdf4llm
from dagster import AssetExecutionContext
from dagster import AssetKey
from dagster import MetadataValue
from dagster import Output
from dagster import asset

from extrato_pipeline.partitions import parse_partition_key
from extrato_pipeline.partitions import statement_partitions
from extrato_pipeline.resources import RawDataSource


def _make_markdown_asset(bank: str, account_type: str):
    @asset(
        key=AssetKey(f"markdown_{bank}_{account_type}"),
        partitions_def=statement_partitions,
        group_name="markdown",
        io_manager_key="text_io_manager",
    )
    def _asset(
        context: AssetExecutionContext, raw_data_source: RawDataSource
    ) -> Output[str]:
        owner, _, month_slug = parse_partition_key(context)

        pdf_path = raw_data_source.get_pdf_path(owner, month_slug, bank, account_type)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        md_text = pymupdf4llm.to_markdown(str(pdf_path))

        return Output(md_text, metadata={"n_chars": MetadataValue.int(len(md_text))})

    return _asset


markdown_itau_debit = _make_markdown_asset("itau", "debit")
markdown_itau_credit = _make_markdown_asset("itau", "credit")
markdown_santander_debit = _make_markdown_asset("santander", "debit")
