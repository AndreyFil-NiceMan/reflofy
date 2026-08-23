"""SkipJob E2E test pipeline: one job is dropped, the sibling job still delivers.

The source produces two records with ``batch_size=1``, so the run plans two
jobs. The record marked ``drop`` raises :class:`SkipJob` — from the destination
hook by default, or from a transformation when ``drop_at="transformation"`` —
and the other job delivers to the mock webhook as usual. The test asserts that
the dropped job is completed (not failed), delivers nothing, and is visible as
such in its job stats.
"""

from typing import Any, Dict, List, Sequence

from typing_extensions import NotRequired

from reflowfy import AbstractPipeline, RuntimeParams, SkipJob, transformation
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from reflowfy.transformations.base import BaseTransformation
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock


class SkipJobParams(RuntimeParams, total=False):
    """Where the drop happens, and a per-run marker to isolate deliveries."""

    drop_at: NotRequired[str]  # "destination" (default) or "transformation"
    marker: NotRequired[str]


@transformation("e2e_drop_job")
def e2e_drop_job(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    """Drop the job from inside a transformation."""
    raise SkipJob(f"transformation dropped record {records[0].get('id')}")


class E2ESkipJobPipeline(AbstractPipeline[SkipJobParams]):
    """Two jobs; the one carrying ``drop: True`` raises SkipJob."""

    name = "e2e_skip_job"

    def define_source(self, runtime_params: SkipJobParams) -> BaseSource:
        marker = runtime_params.get("marker", "none")
        return e2e_mock(
            data=[
                {"id": 1, "marker": marker, "drop": False},
                {"id": 2, "marker": marker, "drop": True},
            ],
            batch_size=1,
        )

    def define_transformations(
        self, records: List[Any], runtime_params: SkipJobParams
    ) -> Sequence[BaseTransformation]:
        # records is empty when the framework only asks for the transformation names.
        if runtime_params.get("drop_at") == "transformation" and self._drops(records):
            return [e2e_drop_job()]
        return []

    def define_destination(
        self, records: List[Any], runtime_params: SkipJobParams
    ) -> BaseDestination:
        if self._drops(records):
            raise SkipJob(f"destination dropped record {records[0]['id']}")
        return e2e_http(body={"records": records})

    @staticmethod
    def _drops(records: List[Any]) -> bool:
        return bool(records) and bool(records[0].get("drop"))
