from pathlib import Path

from dagster import ConfigurableResource


class RawDataSource(ConfigurableResource):
    """Resolve the source raw PDF path for a bronze asset."""

    data_dir: str = "data"

    def get_pdf_path(
        self, owner: str, month_slug: str, bank: str, account_type: str
    ) -> Path:
        return Path(self.data_dir) / owner / f"{month_slug}_{bank}_{account_type}.pdf"
