import boto3
import os
from dotenv import load_dotenv
import json

load_dotenv('.env')

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

textract = boto3.client(
    "textract",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET_NAME = "example-text-extract"


# List all files in the S3 bucket
def list_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    return []

# Process each file in the S3 bucket
files = list_files()
responses = {}

files = list_files()
responses = {}

for file in files:
    print(f"Processing invoice: {file}")

    # Analyze the invoice
    response = textract.analyze_expense(
        Document={"S3Object": {"Bucket": BUCKET_NAME, "Name": file}}
    )

    # Store response for each invoice
    responses[file] = response

    # Save response to a JSON file
    os.makedirs("textract_responses", exist_ok=True)
    file_name = file.replace("/", "_")  # Avoid folder-like names
    with open(f"textract_responses/{file_name}.json", "w") as f:
        json.dump(response, f, indent=4)

    print(f"Saved JSON response for {file}")

print("All invoices processed!")
