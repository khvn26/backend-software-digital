import logging
from decimal import Decimal
from typing import Any, Optional

from dsrs import models, serializers, types

logger = logging.getLogger(__name__)


def map_dsr_row_to_resource(
    dsr_row: dict[str, str], dsr: models.DSR
) -> Optional[types.ResourceKwargs]:
    dsr_row["dsr"] = dsr.id

    # Sensible defaults.
    dsr_row["usages"] = dsr_row.get("usages") or 0
    dsr_row["revenue"] = dsr_row.get("revenue") or Decimal("0.0")

    serializer = serializers.ResourceSerializer(data=dsr_row)
    serializer.is_valid(True)
    return serializer.validated_data


def map_view_data_to_top_resources(
    query_params: dict[str, Any], kwargs: dict[str, str]
) -> types.GetTopResourcesByPercentileKwargs:
    query_serializer = serializers.ResourcePercentileQuerySerializer(data=query_params)
    query_serializer.is_valid(True)
    result = query_serializer.data

    result["territory_code"] = result.pop("territory", None)
    result["percentile"] = int(kwargs["number"]) / 100
    return result
