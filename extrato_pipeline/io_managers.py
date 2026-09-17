from pathlib import Path

from dagster import ConfigurableIOManager
from dagster import InputContext
from dagster import MultiPartitionKey
from dagster import OutputContext


def _partitioned_path(data_dir: str, output_ctx: OutputContext, ext: str) -> Path:
    """Build the on-disk path for a partitioned asset output.

    Multi-dimensional partitions (owner x month), e.g. bronze/markdown assets:
        data/{owner}/{asset_key}/{YYYY_MM}.{ext}
    Single-dimensional partitions (owner only):
        data/{owner}/{asset_key}/totals.{ext}
    """
    asset_key = output_ctx.asset_key.to_user_string()
    partition_key = output_ctx.partition_key

    if isinstance(partition_key, MultiPartitionKey):
        keys = partition_key.keys_by_dimension
        owner = keys["owner"]
        month_slug = keys["month"][:7].replace("-", "_")
        return Path(data_dir) / owner / asset_key / f"{month_slug}.{ext}"

    owner = partition_key
    return Path(data_dir) / owner / asset_key / f"totals.{ext}"


class TextIOManager(ConfigurableIOManager):
    """Dagster IO manager that persists text assets (e.g. markdown) as `.md` files.

    Files are laid out by `_partitioned_path`. Example with
    AssetKey("markdown_itau_debit"), owner=person_1, month=2026-01:
      data/person_1/markdown_itau_debit/2026_01.md
    """

    data_dir: str = "data"

    def handle_output(self, context: OutputContext, obj: str) -> None:
        """Write the asset's text output to its partitioned `.md` path."""
        path = _partitioned_path(self.data_dir, context, "md")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(obj, encoding="utf-8")
        context.add_output_metadata({"path": str(path), "n_chars": len(obj)})

    def load_input(self, context: InputContext) -> str:
        """Read the text produced by the upstream partitioned asset."""
        if context.upstream_output is None:
            raise ValueError("InputContext missing upstream_output")
        return _partitioned_path(
            self.data_dir, context.upstream_output, "md"
        ).read_text(encoding="utf-8")
