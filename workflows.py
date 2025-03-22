from temporalio import workflow, activity
from temporalio.common import RetryPolicy
import datetime
from datetime import timedelta
from models import Params


@workflow.defn
class PdfOCRWorkflow:
    @workflow.run
    async def run(self, params: Params) -> str:
        result_url = await workflow.execute_activity(
            "process_pdf_and_upload_results",
            params,
            schedule_to_close_timeout=datetime.timedelta(hours=1),
            retry_policy=RetryPolicy(
                backoff_coefficient=2.0,
                maximum_attempts=5,
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=2),
            ),
        )

        return result_url 
