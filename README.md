# Temporal OCR Processing Workflow

This repository contains the code used to process the newly released JFK files. It leverages Temporal workflows and activities to perform OCR on PDFs, aggregate results in JSONL format, and upload the processed output to an R2 bucket via S3's API.

## Overview

- **Temporal Workflow**: Orchestrates the processing of PDF files.
- **OCR Processing**: Converts PDFs into images, then uses Apple Vision OCR (via ocrmac) to extract text, and aggregates the results.
- **S3/R2 Integration**: Downloads PDF files from S3, processes them, and uploads the OCR results to an R2 bucket.
- **Key Libraries/Dependencies**:
  - [Temporalio](https://docs.temporal.io/)
  - `boto3` for S3 integrations
  - `pdf2image` for converting PDFs to images
  - `ocrmac` for OCR processing
  - `python-dotenv` for environment variable management

### PS
* this is a great way to acadently spend $200 on anthropic AI models ;)
