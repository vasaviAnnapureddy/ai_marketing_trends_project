import os
from dotenv import load_dotenv
load_dotenv()

missing = [k for k in [
    "YOUTUBE_API_KEY","REDDIT_CLIENT_ID","REDDIT_SECRET","REDDIT_USERNAME","REDDIT_PASSWORD","REDDIT_USERAGENT"
] if not os.getenv(k)]

if missing:
    print("Missing keys:", ", ".join(missing))
else:
    print("All API env vars present.")
