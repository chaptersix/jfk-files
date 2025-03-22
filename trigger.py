import asyncio
import os
import boto3
from datetime import timedelta
from dotenv import load_dotenv
from temporalio.client import Client
from models import Params

load_dotenv()  # Loads environment variables from .env

# R2 setup from environment variables
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
INPUT_FOLDER = "input/"   # Folder in R2 bucket to look for files
OUTPUT_FOLDER = "output/" # Folder in R2 bucket where outputs are stored

def get_s3_client():
    client = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )
    print(f"✅ Connected to bucket '{R2_BUCKET}'.")
    return client

s3_client = get_s3_client()

def list_input_files():
    """
    List all files (keys) in the R2 bucket under the INPUT_FOLDER.
    """
    response = s3_client.list_objects_v2(Bucket=R2_BUCKET, Prefix=INPUT_FOLDER)
    keys = []
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            # Skip if the key represents a folder (ends with a slash)
            if not key.endswith("/"):
                keys.append(key)
    print(f"Found {len(keys)} file(s) in '{INPUT_FOLDER}'.")
    return keys

def output_exists(output_key: str) -> bool:
    """
    Check if the output file already exists in the bucket.
    """
    try:
        s3_client.head_object(Bucket=R2_BUCKET, Key=output_key)  
        return True
    except s3_client.exceptions.ClientError as e:
        # If the error code is 404, the object does not exist.
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise

async def trigger_workflow(client: Client, p: Params):
    # Start the workflow with 1-hour run and task timeouts.
    handle = await client.start_workflow(
        "PdfOCRWorkflow",
        p,
        id=f"pdf-ocr-workflow-{p.output_file_name}",
        task_queue="pdf-ocr-queue",
        execution_timeout=timedelta(hours=1),
    )
    print(f"Workflow started with ID: {handle.id} for file: {p.r2_pdf_url}")

async def main():
    temporal_client = await Client.connect("localhost:7233")
    file_keys = list_input_files()
    
    if not file_keys:
        print("No files found in input folder.")
        return

    # Trigger a workflow for each file sequentially.
    for file_key in file_keys:
        # Extract the base filename from the input key.
        filename = file_key.split("/")[-1]              # e.g., "104-10003-10041.pdf"
        base = os.path.splitext(filename)[0]            # e.g., "104-10003-10041"
        output_key = f"{OUTPUT_FOLDER}{base}.jsonl"       # e.g., "output/104-10003-10041.jsonl"
        
        if output_exists(output_key):
            print(f"Output file {output_key} already exists. Skipping workflow for {file_key}.")
            continue
        
        print(f"Triggering workflow for file: {file_key}, output: {output_key}")
        p = Params(file_key, base)  # Pass base (without extension) as the output_file_name.
        await trigger_workflow(temporal_client, p)

if __name__ == "__main__":
    asyncio.run(main())

