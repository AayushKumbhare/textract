import json
from dotenv import load_dotenv
import boto3
import os
import uuid

load_dotenv('.env')

dynamodb = boto3.client(
    "dynamodb",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

TABLE_NAME = "textract-files"
keys_to_remove_completely = {
        "DocumentMetadata", "Pages", "ExpenseIndex", "SummaryFields", 
        "Confidence", "BoundingBox", "Polygon", "X", "Y", "ResponseMetadata",
        "Geometry", "Id", "Ids", "PageNumber"
    }

# Function to recursively remove specified keys and their entire content from a dictionary or list
def remove_keys_completely(obj, keys_to_remove):
    if isinstance(obj, dict):
        return {
            k: remove_keys_completely(v, keys_to_remove)
            for k, v in obj.items()
            if k not in keys_to_remove
        }
    elif isinstance(obj, list):
        return [remove_keys_completely(item, keys_to_remove) for item in obj]
    else:
        return obj


def cleaning():
    response = dynamodb.scan(TableName=TABLE_NAME)
    items = response.get("Items", [])

    os.makedirs('cleaned_responses', exist_ok=True)

    for item in items:
        try:
            # The data is already JSON string in DynamoDB, parse it to dict
            json_data = json.loads(item["data"]["S"])
            
            # Apply the key removal function directly to the parsed data
            cleaned_data = remove_keys_completely(json_data, keys_to_remove_completely)
            
            # Generate a filename using a timestamp or UUID
            cleaned_filename = f"cleaned_{str(uuid.uuid4())}.json"
            cleaned_filepath = os.path.join("cleaned_responses", cleaned_filename)

            # Save the cleaned data
            with open(cleaned_filepath, "w") as cleaned_file:
                json.dump(cleaned_data, cleaned_file, indent=4)

            print(f"Saved cleaned JSON to {cleaned_filepath}")
            
        except Exception as e:
            print(f"Error processing item: {e}")

if __name__ == "__main__":
    cleaning()
