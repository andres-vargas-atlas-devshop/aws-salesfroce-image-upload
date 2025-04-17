import pandas as pd
import boto3
import requests
import mimetypes
import os
import csv
from urllib.parse import urlparse
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from tqdm import tqdm


# Load credentials
load_dotenv()

sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_SECURITY_TOKEN"),
    domain='login'  # change to 'test' if sandbox
)

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=os.getenv("AWS_REGION")
)

bucket_name = os.getenv("AWS_BUCKET")

# Load Excel file
df = pd.read_csv('accounts_images.csv') # 2 columns: Child External ID, Child Photo URL

# Build Account cache
print("Fetching Account identifiers...")
account_map = {}
query = "SELECT Id, Identifier__c FROM Account WHERE Identifier__c != null LIMIT 20000"
results = sf.query_all(query)
for rec in results['records']:
    account_map[rec['Identifier__c']] = rec['Id']
print(f"✔️ Loaded {len(account_map)} Account records")

# Open CSVs
account_updates = open("account_updates.csv", "w", newline="")
amazon_files = open("amazon_files.csv", "w", newline="")
failures = open("failed.csv", "w", newline="")
successes = open("succeeded.csv", "w", newline="")

acc_writer = csv.writer(account_updates)
file_writer = csv.writer(amazon_files)
fail_writer = csv.writer(failures)
success_writer = csv.writer(successes)

# Headers
acc_writer.writerow(["Id", "Image_Url__c"])
file_writer.writerow(["FileName__c", "Key__c", "MIME__c", "Size__c"])
fail_writer.writerow(["Identifier__c", "Child Photo URL", "Error"])
success_writer.writerow(["Identifier__c", "Child Photo URL", "FileName", "S3_Key", "MIME", "Size"])

# Process each row
for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing Accounts"):
    identifier = str(row["Child External ID"]).strip()
    image_url = str(row["Child Photo URL"]).strip()

    # Validate Account exists
    if identifier not in account_map:
        fail_writer.writerow([identifier, image_url, "Account Identifier__c not found"])
        continue

    account_id = account_map[identifier]

    try:
        # Extract file name from URL
        file_name = os.path.basename(urlparse(image_url).path)
        if not file_name:
            raise ValueError("Filename could not be extracted")

        # Download the image
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        content = response.content
        size = len(content)
        mime_type = response.headers.get("Content-Type") or mimetypes.guess_type(file_name)[0] or "application/octet-stream"

        # S3 Key
        s3_key = f"{account_id}/{file_name}"

        # Upload to S3
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=content, ContentType=mime_type)

        # Write outputs
        s3_url = s3_key  # if using pre-signed links, adapt this line

        acc_writer.writerow([account_id, s3_url])
        file_writer.writerow([file_name, s3_url, mime_type, size])
        success_writer.writerow([identifier, image_url, file_name, s3_url, mime_type, size])

    except Exception as e:
        fail_writer.writerow([identifier, image_url, str(e)])

# Close all files
account_updates.close()
amazon_files.close()
failures.close()
successes.close()

print("🎉 Done! Generated:")
print("🟢 account_updates.csv")
print("🟢 amazon_files.csv")
print("🟢 succeeded.csv")
print("🔴 failed.csv")
