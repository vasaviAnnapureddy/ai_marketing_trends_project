from dotenv import load_dotenv
import os

# This automatically finds .env in the current folder
load_dotenv()

print("GSheet ID:", os.getenv("GSHEET_ID"))
print("Slack Webhook:", os.getenv("SLACK_WEBHOOK"))
print("Slack Signing Secret:", os.getenv("SLACK_SIGNING_SECRET"))
print("Flask App:", os.getenv("FLASK_APP"))

