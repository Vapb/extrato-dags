from dagster import AssetExecutionContext
from dagster import MonthlyPartitionsDefinition
from dagster import MultiPartitionsDefinition
from dagster import StaticPartitionsDefinition

from extrato_pipeline.config import OWNERS

statement_partitions = MultiPartitionsDefinition(
    {
        "owner": StaticPartitionsDefinition(OWNERS),
        "month": MonthlyPartitionsDefinition(start_date="2026-01-01"),
    }
)


def dims(context: AssetExecutionContext) -> tuple[str, str, str]:
    """owner, month (YYYY-MM) and month slug (YYYY_MM) from the partition key."""
    keys = context.partition_key.keys_by_dimension
    owner = keys["owner"]
    month_ym = keys["month"][:7]
    return owner, month_ym, month_ym.replace("-", "_")
