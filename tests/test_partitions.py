import pytest
from dagster import MultiPartitionKey
from dagster import StaticPartitionsDefinition
from dagster import build_asset_context

from extrato_pipeline.config import OWNERS
from extrato_pipeline.partitions import parse_partition_key
from extrato_pipeline.partitions import statement_partitions


def _multi_partition_context(owner: str, month: str):
    return build_asset_context(
        partition_key=MultiPartitionKey({"owner": owner, "month": month})
    )


def _dimension(name: str):
    dims_by_name = statement_partitions.partitions_defs
    return next(d.partitions_def for d in dims_by_name if d.name == name)


@pytest.mark.parametrize(
    ("owner", "month_key", "expected_month_ym", "expected_month_slug"),
    [
        ("person_1", "2026-01-01", "2026-01", "2026_01"),
        ("person_2", "2026-11-15", "2026-11", "2026_11"),
        ("person_1", "2026-12-31", "2026-12", "2026_12"),
    ],
)
def test_parse_partition_key_extracts_owner_month_and_slug(
    owner, month_key, expected_month_ym, expected_month_slug
):
    context = _multi_partition_context(owner, month_key)

    parsed_owner, month_ym, month_slug = parse_partition_key(context)

    assert parsed_owner == owner
    assert month_ym == expected_month_ym
    assert month_slug == expected_month_slug


def test_statement_partitions_owner_dimension_matches_config():
    owner_dim = _dimension("owner")

    assert isinstance(owner_dim, StaticPartitionsDefinition)
    assert owner_dim.get_partition_keys() == OWNERS
