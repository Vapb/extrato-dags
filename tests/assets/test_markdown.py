import pytest
from dagster import AssetKey
from dagster import MultiPartitionKey
from dagster import build_asset_context

from extrato_pipeline.assets import markdown
from extrato_pipeline.assets.markdown import markdown_itau_credit
from extrato_pipeline.assets.markdown import markdown_itau_debit
from extrato_pipeline.assets.markdown import markdown_santander_debit
from extrato_pipeline.resources import RawDataSource


def _context(owner: str, month: str):
    return build_asset_context(
        partition_key=MultiPartitionKey({"owner": owner, "month": month})
    )


@pytest.mark.parametrize(
    ("markdown_asset", "expected_key"),
    [
        (markdown_itau_debit, "markdown_itau_debit"),
        (markdown_itau_credit, "markdown_itau_credit"),
        (markdown_santander_debit, "markdown_santander_debit"),
    ],
)
def test_markdown_asset_key(markdown_asset, expected_key):
    assert markdown_asset.key == AssetKey(expected_key)


def test_markdown_asset_raises_when_pdf_missing(tmp_path):
    context = _context("person_1", "2026-01-01")
    raw_data_source = RawDataSource(data_dir=str(tmp_path))

    with pytest.raises(FileNotFoundError, match="PDF not found"):
        markdown_itau_debit(context=context, raw_data_source=raw_data_source)


def test_markdown_asset_returns_parsed_text_and_metadata(tmp_path, monkeypatch):
    pdf_dir = tmp_path / "person_1"
    pdf_dir.mkdir()
    (pdf_dir / "2026_01_itau_debit.pdf").write_bytes(b"fake pdf content")

    monkeypatch.setattr(markdown.pymupdf4llm, "to_markdown", lambda path: "# statement")

    context = _context("person_1", "2026-01-01")
    raw_data_source = RawDataSource(data_dir=str(tmp_path))

    result = markdown_itau_debit(context=context, raw_data_source=raw_data_source)

    assert result.value == "# statement"
    assert result.metadata["n_chars"].value == len("# statement")
