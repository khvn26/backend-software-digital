from datetime import date
from decimal import Decimal
from typing import Literal, Optional, TypedDict


class DSRFilenameData(TypedDict):
    territory_code: str
    currency_code: str
    period_start: str
    period_end: str


class ResourceKwargs(TypedDict):
    dsp_id: str
    title: str
    artists: str
    isrc: str
    usages: int
    revenue: Decimal


class GetTopResourcesByPercentileKwargs(TypedDict):
    percentile: float
    territory: Optional[str]
    period_start: Optional[date]
    period_end: Optional[date]


DSRStatus = Literal["failed", "ingested"]
