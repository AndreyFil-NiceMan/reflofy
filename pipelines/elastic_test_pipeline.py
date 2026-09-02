"""
Example pipeline for testing Reflofy with Elasticsearch source.

This pipeline:
1. Fetches data from Elasticsearch test index
2. Filters records by status and date range
3. Enriches records with processing metadata
4. Outputs to console for easy testing

Usage:
    # Start Elasticsearch:
    docker compose -f docker-compose.elastic.yml up -d

    # Initialize test data:
    python examples/init_elastic_test_data.py

    # Test locally (synchronous, limited data):
    POST http://localhost:8000/pipelines/elastic_test_pipeline/test
    Query params:
      - start_time: 2024-01-01T00:00:00
      - end_time: 2024-12-31T23:59:59
      - filter_status: active  (optional, defaults to 'active')

    # Run distributed (async via Kafka):
    POST http://localhost:8000/pipelines/elastic_test_pipeline/run
    Same query params as above
"""

import os
from datetime import datetime, timezone
from typing import Literal

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    Transformations,
    BaseDestination,
    Records,
    Param,
    RuntimeParams,
    AbstractPipeline,
    BaseTransformation,
    elastic_source,
    transformation,
)
from reflowfy.destinations.console import console_destination

# ============================================================================
# Params (declared first: the transformation functions below annotate
# runtime_params with this type, so it must exist before they're defined)
# ============================================================================


class ElasticTestParams(RuntimeParams, total=False):
    """Parameters for :class:`ElasticTestPipeline`."""

    start_time: Annotated[Required[str], Param("Start of time range (ISO format)")]
    end_time: Annotated[Required[str], Param("End of time range (ISO format)")]
    filter_status: Annotated[
        NotRequired[Literal["active", "inactive", "pending"]],
        Param("Status to filter by", default="active"),
    ]


# ============================================================================
# Transformations
# ============================================================================


class FilterByStatus(BaseTransformation):
    """Filter records by status field.

    A class, not a @transformation function, because it needs constructor
    state (``allowed_status``) — see EnrichWithProcessingInfo/FormatEventData
    below for the preferred style when no state is needed.
    """

    name = "filter_by_status"

    def __init__(self, allowed_status="active"):
        self.allowed_status = allowed_status

    def apply(self, records, runtime_params):
        """Filter records by status."""
        filter_status = runtime_params.get("filter_status", self.allowed_status)

        filtered = [r for r in records if r["data"].get("status") == filter_status]

        print(f"  📊 Filtered: {len(records)} → {len(filtered)} records (status={filter_status})")

        return filtered


@transformation("enrich_processing_info")
def enrich_processing_info(records: Records, runtime_params: ElasticTestParams) -> Records:
    """Add processing metadata.

    Declared with @transformation (not a BaseTransformation subclass) so
    `runtime_params: ElasticTestParams` is actually checked — a subclass
    can't narrow BaseTransformation.apply's `Dict[str, Any]` to a specific
    TypedDict without mypy/pyright rejecting the override.
    """
    execution_id = runtime_params.get("execution_id", "unknown")
    pipeline_name = runtime_params.get("pipeline_name", "unknown")

    for record in records:
        record["_reflofy_processed"] = {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "processed_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            "framework": "reflofy",
        }

    return records


@transformation("format_event_data")
def format_event_data(records: Records, runtime_params: ElasticTestParams) -> Records:
    """Format event data fields."""
    for record in records:
        event_type = record["data"].get("event_type", "unknown")
        user_name = record["data"].get("user_name", "unknown")
        timestamp = record["data"].get("@timestamp", "unknown")

        record["_summary"] = f"{event_type} by {user_name} at {timestamp}"

        event_data = record["data"].get("event_data", {})
        if event_type == "purchase" and "amount" in event_data:
            event_data["formatted_amount"] = f"${event_data['amount']:.2f}"

    return records


# ============================================================================
# Pipeline Definition
# ============================================================================


class ElasticTestPipeline(AbstractPipeline[ElasticTestParams]):
    """
    Elasticsearch-based test pipeline.

    Demonstrates:
    - Dynamic source configuration with runtime parameters
    - Conditional filtering based on parameters
    - Exposed parameters for API documentation
    """

    name = "elastic_test_pipeline"
    rate_limit = 20

    def define_source(self, runtime_params):
        """Configure Elasticsearch source with runtime parameters."""
        elasticsearch_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

        return elastic_source(
            url=elasticsearch_url,
            index="reflofy-test-data",
            base_query={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "{{ start_time }}",
                                        "lte": "{{ end_time }}",
                                    }
                                }
                            }
                        ],
                    }
                },
                "sort": [{"@timestamp": {"order": "desc"}}],
            },
            scroll="2m",
            size=1,
        )

    def define_destination(
        self, records: Records, runtime_params: ElasticTestParams
    ) -> BaseDestination:
        """Configure console destination."""
        return console_destination(
            pretty_print=True,
            max_records_display=10,
        )

    def define_transformations(
        self, records: Records, runtime_params: ElasticTestParams
    ) -> Transformations:
        """Build transformation pipeline."""
        filter_status = runtime_params.get("filter_status", "active")

        return [
            FilterByStatus(allowed_status=filter_status),
            enrich_processing_info(),
            format_event_data(),
        ]
