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
client = OpenAI(api_key=api_key)

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
    Reads all JSON files from 'cleaned_responses' directory and loads their content.
    """
    parsed_data = []
    directory = "cleaned_responses"

    if not os.path.exists(directory):
        print("Directory 'cleaned_responses' does not exist.")
        return []

    files = os.listdir(directory)
    
    for file in files:
        if file.endswith(".json"):
            file_path = os.path.join(directory, file)

            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                parsed_data.append({"file_name": file, "data": data})
            except json.JSONDecodeError as e:
                print(f"Error reading {file}: {e}")
                continue

    print(f"Found {len(parsed_data)} financial reports.")
    return parsed_data


def process_financial_data(financial_data):
    """
    Processes the financial data through OpenAI's GPT to extract structured data.
    """
    structured_data = []

    with open('company-detail-template.json', 'r') as f:
        company_detail_template = json.load(f)

    for doc in financial_data:
        file_name = doc["file_name"]
        data = doc["data"]

        print(f"Processing {file_name}...")

        try:
            completion = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": f"""You are a financial data extraction assistant.
                        Extract financial information from this JSON and structure it into a standardized format.
                        The output MUST be valid JSON format.
                     The output MUST follow the following JSON template in order to best enhance the user experience
                     and make it easy to understand and follow along.
                    ''' json:
                     {json.dumps(company_detail_template, indent=4)}
                     '''
                     """},
                    {"role": "user", "content": json.dumps(data)}
                ]
            )

            structured_json = completion.choices[0].message.content


            try:
                # Try to parse the JSON response
                parsed_json = json.loads(structured_json)
                structured_data.append({
                    "file_name": file_name, 
                    "structured_data": parsed_json
                })
                print(f"Successfully processed {file_name}")
            except json.JSONDecodeError as je:
                print(f"Invalid JSON response for {file_name}")
                print(f"JSON Error: {je}")

        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            continue

    return structured_data


def save_as_csv(structured_data):
    """
    Saves structured JSON data as CSV files in 'financial_reports' directory.
    """
    os.makedirs("financial_reports", exist_ok=True)

    for doc in structured_data:
        file_name = doc["file_name"].replace(".json", ".csv")
        data = doc["structured_data"]

        try:
            df = pd.json_normalize(data)
            csv_path = f"financial_reports/{file_name}"
            df.to_csv(csv_path, index=False)
            print(f"Saved CSV: {csv_path}")
        except Exception as e:
            print(f"Error saving {file_name} to CSV: {e}")


# Execute the process
financial_data = get_financial_data()
if financial_data:
    structured_data = process_financial_data(financial_data)
    if structured_data:
        save_as_csv(structured_data)
        print("All financial reports saved as CSV!")
    else:
        print("No structured data extracted.")
else:
    print("No financial data found in 'cleaned_responses'.")
