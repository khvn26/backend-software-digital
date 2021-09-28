import csv
import logging
import re
from datetime import date, datetime
from io import TextIOWrapper
from itertools import islice
from typing import Generator, Optional

from django.conf import settings
from django.core.files import File
from django.core.files.storage import get_storage_class
from django.db import DatabaseError
from django.db.models.query import QuerySet
from rest_framework.exceptions import ValidationError

from dsrs.mappers import map_dsr_row_to_resource
from dsrs.models import DSR, Currency, Resource, Territory
from dsrs.types import DSRFilenameData

logger = logging.getLogger(__name__)


# No strict metadata format requirements were given, so just generalize what we saw in
# the filenames.
FILENAME_REGEX: re.Pattern = re.compile(
    r"(?P<territory_code>[A-Z]{2})_(?P<currency_code>[A-Z]{3})_(?P<period_start>[0-9]{8})-(?P<period_end>[0-9]{8})"
)

FILENAME_DATE_FORMAT: str = "%Y%m%d"


def get_dsr(parsed_data: DSRFilenameData) -> Optional[DSR]:
    """
    Get an unsaved DSR instance from metadata parsed from filename.
    """
    kwargs = {}

    currency_code = parsed_data["currency_code"]
    currency, _ = Currency.objects.get_or_create(code=currency_code)
    kwargs["currency"] = currency

    territory_code = parsed_data["territory_code"]
    territory, _ = Territory.objects.get_or_create(
        code_2=territory_code, defaults={"local_currency": currency}
    )
    kwargs["territory"] = territory

    for kwarg in ("period_start", "period_end"):
        try:
            kwargs[kwarg] = datetime.strptime(
                parsed_data[kwarg], FILENAME_DATE_FORMAT
            ).date()
        except ValueError:
            return None

    return DSR(**kwargs)


def save_dsr_file(dsr_file: File) -> str:
    """
    Save DSR file and get its relative path.
    """
    storage = get_storage_class()()
    name = storage.save(content=dsr_file, name=None)
    return name


def parse_filename(filename: str) -> Optional[DSRFilenameData]:
    match = FILENAME_REGEX.search(filename)
    return match and match.groupdict()


def iter_resources(
    dsr_file: File, dsr: DSR
) -> Generator[Optional[Resource], None, None]:
    with TextIOWrapper(dsr_file) as fp:
        reader = csv.DictReader(
            fp,
            fieldnames=["dsp_id", "title", "artists", "isrc", "usages", "revenue"],
            dialect="excel-tab",
        )
        # Skip header row
        next(reader, None)
        for row in reader:
            try:
                kwargs = map_dsr_row_to_resource(dsr_row=row, dsr=dsr)
            except ValidationError as exc:
                logger.error("Could not map row %s for DSR %s: %s", row, dsr, exc)
                yield None
            else:
                yield Resource(**kwargs)


def ingest_dsr(dsr: DSR) -> None:
    """
    Ingest DSR file and save resulting resources.
    Assign ingestion status to the DSR instance.
    """
    dsr_file = get_storage_class()().open(dsr.path)
    batch_size = settings.DSR_RESOURCE_IMPORT_BATCH_SIZE
    resources = iter_resources(dsr_file=dsr_file, dsr=dsr)
    final_status = "ingested"
    while True:
        try:
            batch = list(islice(resources, batch_size))
            batch_without_fails = list(filter(None, batch))
            if len(batch) != len(batch_without_fails):
                final_status = "failed"
            if not batch:
                dsr.status = final_status
                dsr.save(update_fields=["status"])
                return
            Resource.objects.bulk_create(batch_without_fails, batch_size)
        except (OSError, csv.Error, DatabaseError) as exc:
            logger.error("Error ingesting %s: %s", dsr_file, exc, exc_info=exc)
            return


# Public services below.


def import_dsr(dsr_file: File) -> Optional[DSR]:
    """
    Parse the uploaded file's filename. If valid, store the DSR
    and ingest it immediately.
    """
    # DRF parser guarantees file.name presence, but we want to be safe.
    if not dsr_file.name:
        return None  # pragma: no cover

    parsed_data = parse_filename(dsr_file.name)
    if not parsed_data:
        return None

    dsr = get_dsr(parsed_data)
    if not dsr:
        return None

    dsr.path = save_dsr_file(dsr_file)
    dsr.save()

    ingest_dsr(dsr=dsr)

    return dsr


def get_top_resources_by_percentile(
    percentile: float,
    territory_code: Optional[str] = None,
    period_start: Optional[date] = None,
    period_end: Optional[date] = None,
) -> QuerySet:
    """
    Find the top percentile by revenue. Optionally, narrow results
    to a specific territory and/or date boundaries.
    """
    dsr_filter = {}
    if territory_code:
        dsr_filter["territory__code_2"] = territory_code
    if period_start:
        dsr_filter["period_start__gte"] = period_start
    if period_end:
        dsr_filter["period_end__lte"] = period_end
    dsr_ids = DSR.objects.filter(**dsr_filter).values_list("id", flat=True)
    # TODO Requirements specify response currency as EUR; revenue currency
    # conversion is something that should be done during ingestion and requires
    # historical data to actually make sense.
    #
    # Not sure if resulting data will be useful, but
    # a somewhat plausible way to do this would consist of following:
    # - A table with rates by day, partitioned by date
    # - A scheduled worker responsible for updates
    # - A query to approximate the average rate between period_start and period_end
    # - Possibly a materialized view with rates by DSR ids
    # - Conversion during ingestion
    # - Store original currencies/revenues in separate columns
    #
    # This is something that I feel is out of scope of the task at hand,
    # so for now just naively add it up
    return Resource.objects.raw(
        """
    WITH aggregated_resources AS (
        SELECT
            dsrs_resource.dsp_id,
            dsrs_resource.title,
            dsrs_resource.artists,
            dsrs_resource.isrc, 
            ARRAY_AGG(dsrs_resource.dsr_id) AS dsr_ids,
            SUM(dsrs_resource.usages) AS usages,
            SUM(dsrs_resource.revenue) AS revenue,
            PERCENT_RANK() OVER (ORDER BY SUM(revenue) DESC) AS percentile
        FROM dsrs_resource
        WHERE dsrs_resource.dsr_id = ANY(%s)
        GROUP BY
            dsrs_resource.dsp_id,
            dsrs_resource.title,
            dsrs_resource.artists, 
            dsrs_resource.isrc
    )
    SELECT
        dsp_id as id, dsp_id, title, artists, isrc, dsr_ids, usages, revenue
    FROM aggregated_resources
    WHERE aggregated_resources.percentile <= %s;
    """,
        [list(dsr_ids), percentile],
    )
