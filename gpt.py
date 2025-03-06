import os
import json
import pandas as pd
from dotenv import load_dotenv
import boto3
from openai import OpenAI  

# Load environment variables
load_dotenv('.env')

# OpenAI API Client
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()

# DynamoDB Client
dynamodb = boto3.client(
    "dynamodb",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Fetch JSON template for output structure
with open("company-detail-template.json", "r") as f:
    company_detail_template_json = f.read()

# DynamoDB table name
TABLE_NAME = "textract-files"


def get_financial_data():
    """
    Fetch all stored financial data JSONs from DynamoDB.
    Returns: A list of parsed JSON data.
    """
    print("📡 Fetching data from DynamoDB...")

    response = dynamodb.scan(TableName=TABLE_NAME)
    items = response.get("Items", [])

    parsed_data = []
    for item in items:
        file_name = item["id"]["S"]
        json_data = json.loads(item["data"]["S"])  # Convert string back to JSON
        parsed_data.append({"file_name": file_name, "data": json_data})

    print(f"✅ Retrieved {len(parsed_data)} financial documents from DynamoDB.")
    return parsed_data


def process_financial_data(financial_data):
    """
    Sends financial data to GPT for structuring.
    Returns: List of structured JSON responses.
    """
    structured_data = []

    for doc in financial_data:
        file_name = doc["file_name"]
        data = doc["data"]

        print(f"🔍 Processing {file_name}...")

        try:
            completion = client.chat.completions.create(
                model="gpt-4-0125-preview",  # Make sure you're using a valid model
                messages=[
                    {"role": "system", "content": f"""You are a financial data extraction assistant.
                        Extract financial information from this JSON and structure it into a standardized format.
                        The output MUST be valid JSON format."""},
                    {"role": "user", "content": json.dumps(data)}
                ]
            )

            structured_json = completion.choices[0].message.content
            
            # Debug print
            print("Raw GPT response:")
            print(structured_json)
            
            try:
                # Try to parse the JSON response
                parsed_json = json.loads(structured_json)
                structured_data.append({
                    "file_name": file_name, 
                    "structured_data": parsed_json
                })
                print(f"✅ Successfully processed {file_name}")
            except json.JSONDecodeError as je:
                print(f"❌ Invalid JSON response for {file_name}")
                print(f"JSON Error: {je}")
                # You might want to add some fallback handling here
                
        except Exception as e:
            print(f"❌ Error processing {file_name}: {e}")
            continue

    return structured_data


def save_as_csv(structured_data):
    """
    Saves structured JSON data as CSV files.
    """
    os.makedirs("financial_reports", exist_ok=True)

    for doc in structured_data:
        file_name = doc["file_name"].replace(".json", ".csv")
        data = doc["structured_data"]

        df = pd.json_normalize(data)  # Convert JSON to DataFrame
        csv_path = f"financial_reports/{file_name}"
        df.to_csv(csv_path, index=False)

        print(f"📂 Saved CSV: {csv_path}")


# 🔥 Full Automation Pipeline
financial_data = get_financial_data()
structured_data = process_financial_data(financial_data)
save_as_csv(structured_data)

print("🚀 All financial reports saved as CSV!")