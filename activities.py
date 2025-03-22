import os
import json
import tempfile
import boto3
from dotenv import load_dotenv
from temporalio import activity
from models import Params
from pdf2image import convert_from_path
from ocrmac import ocrmac

load_dotenv()  # Loads the .env file automatically

# R2 setup
R2_ENDPOINT = os.getenv('R2_ENDPOINT')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY')
R2_SECRET_KEY = os.getenv('R2_SECRET_KEY')
R2_BUCKET = os.getenv('R2_BUCKET')
OUTPUT_FOLDER = 'output'


def get_s3_client():
    client = boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )
    print(f"✅ Successfully created S3 client for bucket '{R2_BUCKET}'.")
    return client

s3_client = get_s3_client()
def download_pdf_from_s3(key: str, dest_dir: str) -> str:
    local_pdf_path = os.path.join(dest_dir, 'input.pdf')
    s3_client.download_file(R2_BUCKET, key, local_pdf_path)
    print(f"Downloaded PDF from S3 key '{key}' to '{local_pdf_path}'.")
    return local_pdf_path


def pdf_to_images(pdf_path: str, output_dir: str) -> list:
    image_paths = []
    print(f"Converting PDF: {pdf_path}")
    images = convert_from_path(pdf_path)
    print(f"Extracted {len(images)} pages from PDF")
    
    for i, image in enumerate(images):
        image_path = os.path.join(output_dir, f'page_{i}.png')
        image.save(image_path, 'PNG')
        print(f"Saved page {i+1} to '{image_path}'.")
        image_paths.append(image_path)
    
    return image_paths


def apple_vision_ocr(image_path: str) -> dict:
    print(f"Processing image: {image_path}")
    # Call OCR with language preference and convert output list to a dict
    ocr_results = ocrmac.OCR(image_path, language_preference=['en-US']).recognize()
    results = []
    for text, confidence, bbox in ocr_results:
        results.append({
            "text": text,
            "confidence": confidence,
            "bbox": bbox
        })
    return {"results": results}


def run_apple_vision_ocr(image_paths: list) -> dict:
    results = {}
    for img_path in image_paths:
        ocr_data = apple_vision_ocr(img_path)
        results[os.path.basename(img_path)] = ocr_data
        os.remove(img_path)  # Cleanup image after processing
    return results


def upload_result_to_r2(file_name: str, content: str) -> str:
    # Change extension to .jsonl for json lines format
    output_key = f"{OUTPUT_FOLDER}/{file_name}.jsonl"
    s3_client.put_object(Bucket=R2_BUCKET, Key=output_key, Body=content.encode('utf-8'))
    uploaded_url = f"{R2_ENDPOINT}/{R2_BUCKET}/{output_key}"
    print(f"Uploaded OCR result to '{uploaded_url}'.")
    return uploaded_url


@activity.defn
def process_pdf_and_upload_results(params: Params) -> str:
    # Use a single temporary directory for the entire process.
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the PDF into the temporary directory.
        pdf_path = download_pdf_from_s3(params.r2_pdf_url, temp_dir)
        print("PDF downloaded.")

        # Create a subdirectory for images.
        images_dir = os.path.join(temp_dir, "images")
        os.makedirs(images_dir, exist_ok=True)
        image_paths = pdf_to_images(pdf_path, images_dir)
        print("Converted PDF to images.")

        ocr_results = run_apple_vision_ocr(image_paths)
        print("Completed OCR processing on images.")

        # Aggregate OCR results in JSONL format with page numbers
        jsonl_lines = []
        for filename, data in ocr_results.items():
            try:
                # Expecting filename format: "page_{i}.png"
                page_str = filename.split('_')[1].split('.')[0]
                page_number = int(page_str) + 1  # Convert to 1-indexed page number
            except Exception as e:
                print(f"Error extracting page number from {filename}: {e}")
                page_number = None
            record = {
                "page": page_number,
                "results": data["results"]
            }
            jsonl_lines.append(json.dumps(record))
        aggregated_text = "\n".join(jsonl_lines)
        print("Aggregated OCR results in JSONL format.")

        # Upload the aggregated JSONL text back to R2.
        outname = params.output_file_name.replace(".pdf","").replace(".jsonl", "")
        result_url = upload_result_to_r2(outname, aggregated_text)
        print("Uploaded OCR results to R2.")

    return result_url

