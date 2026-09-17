from pathlib import Path

import pytest
from dagster import AssetKey
from dagster import MultiPartitionKey
from dagster import build_input_context
from dagster import build_output_context

from extrato_pipeline.io_managers import TextIOManager
from extrato_pipeline.io_managers import _partitioned_path


def _output_context(asset_key: str, partition_key):
    return build_output_context(
        asset_key=AssetKey(asset_key), partition_key=partition_key
    )


@pytest.mark.parametrize(
    ("asset_key", "partition_key", "ext", "expected"),
    [
        (
            "markdown_itau_debit",
            MultiPartitionKey({"owner": "person_1", "month": "2026-01-01"}),
            "md",
            "data/person_1/markdown_itau_debit/2026_01.md",
        ),
        (
            "report",
            "person_1",
            "csv",
            "data/person_1/report/totals.csv",
        ),
    ],
)
def test_partitioned_path_builds_expected_layout(
    asset_key, partition_key, ext, expected
):
    context = _output_context(asset_key, partition_key)

    path = _partitioned_path("data", context, ext)

    assert path == Path(expected)


def test_text_io_manager_handle_output_writes_file_and_metadata(tmp_path):
    manager = TextIOManager(data_dir=str(tmp_path))
    partition_key = MultiPartitionKey({"owner": "person_1", "month": "2026-01-01"})
    context = _output_context("markdown_itau_debit", partition_key)

    manager.handle_output(context, "hello world")

    expected_path = tmp_path / "person_1" / "markdown_itau_debit" / "2026_01.md"
    assert expected_path.read_text(encoding="utf-8") == "hello world"

    metadata = context.get_logged_metadata()
    assert metadata["path"].text == str(expected_path)
    assert metadata["n_chars"].value == len("hello world")


def test_text_io_manager_load_input_reads_upstream_file(tmp_path):
    manager = TextIOManager(data_dir=str(tmp_path))
    partition_key = MultiPartitionKey({"owner": "person_1", "month": "2026-01-01"})
    output_context = _output_context("markdown_itau_debit", partition_key)
    manager.handle_output(output_context, "hello world")

    input_context = build_input_context(upstream_output=output_context)

    assert manager.load_input(input_context) == "hello world"


def test_text_io_manager_load_input_raises_without_upstream_output():
    manager = TextIOManager()
    input_context = build_input_context()

    with pytest.raises(ValueError, match="upstream_output"):
        manager.load_input(input_context)
