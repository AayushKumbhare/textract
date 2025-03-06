import os
from dotenv import load_dotenv
from openai import OpenAI 

load_dotenv('.env')
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()

with open("company_detail_template.json", "r") as f:
    company_detail_template_json = f.read()

completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": """You are a finanical data extraction assistant:
                    You are a financial data extraction assistant specializing in structured financial data organization.

                    ### **Objective**:
                    Extract **financial information** from the provided JSON data (derived from financial statements, cap tables, and term sheets) and structure it into a standardized **company tearsheet format**.

                    ---

                    ### **Instructions**:
                    1. **Use this JSON format for output:**
                    {company_detail_template_json}

                    2. **Source Prioritization**:
                    - **User-uploaded financial documents take priority** over any previously extracted or online data.
                    - **If a field is missing in the documents but available online, retain the online source**.
                    - **Ensure consistency**: if extracted funding rounds do not match total funding, flag it.

                    3. **Data Mapping & Standardization**:
                    - Identify relevant **financial fields** (e.g., revenue, EBITDA, funding rounds) and map them to the corresponding sections.
                    - For **nested fields** (e.g., `ebitda`, `fundingRounds`, `partnerships`), ensure proper JSON formatting.
                    - Convert all **currency values to a standard format** (e.g., USD).
                    - Represent **percentages as decimals** (e.g., `"12.5%" → 0.125`).

                    4. **Citations & References**:
                    - If a value is extracted from a **user-uploaded document**, add `{ "source": "User-Uploaded Document" }` to `"citations"`.
                    - If a value is **retained from an online source**, include the **original URL** in `"citations"`.

                    5. **Validation & Error Handling**:
                    - Ensure logical consistency (e.g., total funding must equal the sum of funding rounds).
                    - If conflicting values exist between sources, flag them for review.
                            
        """}
    ]
)

print(completion.choices[0].message)

'''



'''
