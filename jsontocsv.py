import os
import json
import pandas as pd

def parse_textract_json(json_file):
    """
    Parses an AWS Textract Expense Analysis JSON file and extracts expense details into a structured format.
    
    :param json_file: Path to the JSON file
    :return: A list of extracted transactions (each transaction as a dictionary)
    """
    with open(json_file, "r") as f:
        data = json.load(f)

    transactions = []

    # Check if ExpenseDocuments exist
    if "ExpenseDocuments" in data:
        for expense_doc in data["ExpenseDocuments"]:
            date = None
            vendor_name = None
            total = None

            # Extract SummaryFields (Top-Level Fields)
            for field in expense_doc.get("SummaryFields", []):
                field_type = field["Type"]["Text"]
                field_value = field.get("ValueDetection", {}).get("Text", "")

                if field_type == "INVOICE_RECEIPT_DATE":
                    date = field_value
                elif field_type == "VENDOR_NAME":
                    vendor_name = field_value
                elif field_type == "TOTAL":
                    total = field_value

            # Extract Line Items (Detailed Purchases)
            for group in expense_doc.get("LineItemGroups", []):
                for line_item in group.get("LineItems", []):
                    item_desc = None
                    item_price = None
                    for field in line_item.get("LineItemExpenseFields", []):
                        if field["Type"]["Text"] == "ITEM":
                            item_desc = field["ValueDetection"]["Text"]
                        elif field["Type"]["Text"] == "PRICE":
                            item_price = field["ValueDetection"]["Text"]

                    if item_desc and item_price:
                        transactions.append({
                            "Date": date if date else "Unknown",
                            "Vendor": vendor_name if vendor_name else "Unknown",
                            "Description": item_desc,
                            "Amount": item_price
                        })

            # If no line items, use SummaryFields
            if vendor_name and total:
                transactions.append({
                    "Date": date if date else "Unknown",
                    "Vendor": vendor_name,
                    "Description": "Total Expense",
                    "Amount": total
                })

    return transactions


def process_all_jsons(input_folder, output_csv):
    """
    Reads all AWS Textract Expense JSON files from a folder, extracts expenses, and saves them into a CSV.
    
    :param input_folder: Folder containing JSON files
    :param output_csv: Output CSV file name
    """
    all_transactions = []

    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)
            transactions = parse_textract_json(file_path)
            all_transactions.extend(transactions)

    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(all_transactions)
    df.to_csv(output_csv, index=False)
    print(f"Expense report saved as: {output_csv}")

# Example Usage
if __name__ == "__main__":
    input_folder = "/Users/aayushkumbhare/Desktop/textract/textract_responses"  # Update this path
    output_csv = "expense_report.csv"

    process_all_jsons(input_folder, output_csv)
