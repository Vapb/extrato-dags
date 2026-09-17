from pathlib import Path

from extrato_pipeline.resources import RawDataSource


def test_get_pdf_path_builds_expected_layout():
    source = RawDataSource(data_dir="data")

    path = source.get_pdf_path(
        owner="person_1", month_slug="2026_01", bank="itau", account_type="debit"
    )

    assert path == Path("data/person_1/2026_01_itau_debit.pdf")


def test_get_pdf_path_uses_configured_data_dir():
    source = RawDataSource(data_dir="/tmp/raw")

    path = source.get_pdf_path(
        owner="person_2", month_slug="2026_02", bank="santander", account_type="debit"
    )

    assert path == Path("/tmp/raw/person_2/2026_02_santander_debit.pdf")


def test_get_pdf_path_defaults_to_data_dir():
    source = RawDataSource()

    assert source.data_dir == "data"
