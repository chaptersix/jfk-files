import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from workflows import PdfOCRWorkflow
from concurrent.futures import ThreadPoolExecutor
import activities

async def main():
    client = await Client.connect("localhost:7233")  # Adjust if needed

    worker = Worker(
        client,
        task_queue="pdf-ocr-queue",
        workflows=[PdfOCRWorkflow],
        activities=[
            activities.process_pdf_and_upload_results,
        ],
        activity_executor=ThreadPoolExecutor(max_workers=10),
    )

    print("Temporal Worker started and running...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
