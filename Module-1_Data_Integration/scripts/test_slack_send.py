import os
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SLACK_WEBHOOK") or os.getenv("slack_webhook")
print("Using webhook:", (url[:60] + "...") if url else None)

payload = {
    "text": "✅ *Slack test message* — If you see this, your pipeline can send messages!"
}

response = requests.post(url, json=payload)

print("Status:", response.status_code)
print("Response text:", response.text)
