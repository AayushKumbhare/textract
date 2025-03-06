import boto3
import os
from dotenv import load_dotenv
import json
import uuid

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

            response = textract.get_document_analysis(JobId=job_id["JobId"])
            responses[file] = response

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
