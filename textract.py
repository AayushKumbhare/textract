import boto3
import os
from dotenv import load_dotenv
import json
import uuid
import time

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

dynamodb = boto3.client(
    "dynamodb",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)


# List all files in the S3 bucket
def list_files():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"]]
    return []


def add_to_dynamodb(file):
    try:
        with open(f'textract_responses/{file}', 'r') as f:
            data = json.load(f)
        data_string = json.dumps(data)
        data_size = len(data_string.encode('utf-8'))
        if (data_size > 1024 * 1024):
            dynamodb.put_item(
                TableName='textract-files',
                Item={
                    'id': {'S': file},
                    'id_2': {'S': str(uuid.uuid4())},
                    'data': {'S': 'file too large'}
                }
            )
            print(f"{file} was too large")
        else:
            dynamodb.put_item(
                TableName='textract-files',
                Item={
                    'id': {'S': file},
                    'id_2': {'S': str(uuid.uuid4())},
                    'data': {'S': json.dumps(data)}
                }
            )
        print(f"Added {file} to DynamoDB")
    except Exception as e:
        print(f"Error adding {file} to DynamoDB: {e}")

def get_document_analysis_results(job_id):
    max_retries = 30  # Adjust based on your needs
    retry_count = 0
    
    while retry_count < max_retries:
        response = textract.get_document_analysis(JobId=job_id)
        status = response['JobStatus']
        
        if status == 'SUCCEEDED':
            return response
        elif status == 'FAILED':
            print(f"Analysis job failed: {response.get('StatusMessage', 'No error message available')}")
            return None
        elif status == 'IN_PROGRESS':
            print(f"Analysis in progress... (Attempt {retry_count + 1}/{max_retries})")
            time.sleep(5)  # Wait 5 seconds before checking again
            retry_count += 1
        else:
            print(f"Unexpected status: {status}")
            return None
    
    print("Maximum retries reached. Job took too long.")
    return None

def extract_data():
    # Process each file in the S3 bucket
    files = list_files()
    responses = {}

    for file in files:
        if not file.endswith(".pdf"):
            print(f"Skipping {file} (Not a pdf)")
            continue
        print(f"Processing invoice: {file}")

        try:
            # Analyze the invoice
            job_id = textract.start_document_analysis(
                DocumentLocation= {
                    "S3Object": {
                        "Bucket": BUCKET_NAME,
                        "Name": file
                    }
                },
                FeatureTypes=["FORMS", "TABLES"]
            )

            response = get_document_analysis_results(job_id["JobId"])
            if response:
                responses[file] = response
            else:
                print("Analysis failed or timed out")

            os.makedirs("textract_responses", exist_ok=True)
            file_name = file.replace("/", "_")
        
            with open(f"textract_responses/{file_name}.json", "w") as f:
                json.dump(response, f, indent=4)
            print(f"Saved JSON response for {file} in directory textract_responses")

            add_to_dynamodb(f"{file_name}.json")

            s3.delete_object(Bucket=BUCKET_NAME, Key=file)
            print(f"Deleted {file} from S3 bucket")
        except Exception as e:
            print(f"Error processing {file}: {e}")


    print("All invoices processed!")
    return True if responses else False


if __name__ == "__main__":
    extract_data()
