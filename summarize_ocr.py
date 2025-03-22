import anthropic
import json
import os
import boto3
import hashlib
import re

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
INPUT_FOLDER = "input/"   # Folder in R2 bucket to look for files
OUTPUT_FOLDER = "output/" # Folder in R2 bucket where outputs are stored

client = anthropic.Anthropic(
    # defaults to os.environ.get("ANTHROPIC_API_KEY")
    api_key=os.environ.get("ANTHROPIC_API_KEY"),
)

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

def list_output_jsonl_files():
    """
    List all JSONL files in the R2 bucket under the OUTPUT_FOLDER.
    """
    response = s3_client.list_objects_v2(Bucket=R2_BUCKET, Prefix=OUTPUT_FOLDER)
    keys = []
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            # Skip if the key represents a folder (ends with a slash) or isn't a jsonl file
            if not key.endswith("/") and key.endswith(".jsonl"):
                keys.append(key)
    return keys

def get_file_contents(file_key):
    """
    Get the contents of a file from the R2 bucket.
    
    Args:
        file_key (str): The full key (path) of the file in the bucket
        
    Returns:
        str: The contents of the file
    """
    try:
        response = s3_client.get_object(Bucket=R2_BUCKET, Key=file_key)
        contents = response['Body'].read().decode('utf-8')
        return contents
    except Exception as e:
        print(f"Error reading file {file_key}: {e}")
        return None


def process_jsonl_file(file_key):
    print(f"\n📄 Processing R2 file: {file_key}")
    
    # Get file contents from R2
    contents = get_file_contents(file_key)
    if not contents:
        print(f"❌ Failed to get contents of {file_key}")
        return []
    
    batch_requests = []
    
    # Process each line in the file contents
    for i, line in enumerate(contents.splitlines()):
        if not line.strip():  # Skip empty lines
            continue
            
        data = json.loads(line)
        # Extract text from all results and join them
        all_text = "\n".join([result["text"] for result in data["results"]])
        
        # Create a custom_id using hash that conforms to '^[a-zA-Z0-9_-]{1,64}$'
        hash_input = f"{file_key}_{i}".encode('utf-8')
        custom_id = hashlib.md5(hash_input).hexdigest()
        # Validate the custom_id matches the required pattern
        if not re.match('^[a-zA-Z0-9_-]{1,64}$', custom_id):
            raise ValueError(f"Generated custom_id {custom_id} does not match required pattern")
        batch_requests.append({
            "custom_id": custom_id,
            "params": {
                "model": "claude-3-7-sonnet-20250219",
                "max_tokens": 20000,
                "temperature": 1,
                "system": "You are a government intelligence analyst whose primary goal is to communicate the facts clearly and accurately. You will be provided with OCR-scanned documents that require analysis. Your task is to produce a detailed, factual report that includes the following sections:\n* Title of the Document, Document Identifier, and Date of the document should be on sepeerate lines starting at the beginning of the document\n* Government Agencies: Identify and list all government agencies mentioned in the document. Provide sub bullets for each one describing their involvement \n* Document Description: Provide a concise description of the document's nature, including its purpose, classification, and any contextual details that can be inferred.\n* Entities Mentioned: Extract and list every name, country, organization, and leader mentioned in the document. For each individual entity, write one sentence describing its presence or role within the document.\n* OCR Errors: Identify and list any OCR errors present in the text, along with your interpretation of the intended text.\n* Assumptions: Clearly enumerate any assumptions you are making during your analysis, such as corrections of OCR errors or contextual inferences based on the document's layout.\n* Re-write the document in a more clear format. Correct any OCR errors found\n* Do not include any concluding summary or meta-commentary about the document analysis\n* Ensure that your analysis is strictly factual, objective, and precise, focusing solely on the content provided in the document.\n* separate each section with markdown separators",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": all_text
                            }
                        ]
                    }
                ]
            }
        })
    
    return batch_requests

def main():
    print("\n🔍 Looking for JSONL files in R2 bucket...")
    jsonl_files = list_output_jsonl_files()
    
    if not jsonl_files:
        print("❌ No JSONL files found in the output folder!")
        return
        
    print(f"\n✨ Found {len(jsonl_files)} files to process:")
    for file_key in jsonl_files:
        print(f"  📎 {file_key}")
    
    print("\n🚀 Starting processing...")
    
    # Collect all batch requests
    all_batch_requests = []
    for file_key in jsonl_files:
        batch_requests = process_jsonl_file(file_key)
        all_batch_requests.extend(batch_requests)
    
    if all_batch_requests:
        print(f"\n📦 Submitting batch request with {len(all_batch_requests)} documents...")
        batch_response = client.beta.messages.batches.create(
            requests=all_batch_requests
        )
        print(f"\n🆔 Batch ID: {batch_response}")
    
    print("\n✅ Processing complete!")


if __name__ == "__main__":
    main()

