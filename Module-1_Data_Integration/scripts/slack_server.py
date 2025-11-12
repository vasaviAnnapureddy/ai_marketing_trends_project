# slack_server.py
from flask import Flask, request, jsonify, abort
import os, json, hmac, hashlib, time
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
from sheets_utils import append_row

SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET","")
app = Flask(__name__)

def verify(req):
    if not SLACK_SIGNING_SECRET:
        return True
    timestamp = req.headers.get("X-Slack-Request-Timestamp","0")
    if abs(time.time() - int(timestamp)) > 60*5:
        return False
    sig_basestring = f"v0:{timestamp}:{req.get_data(as_text=True)}"
    my_sig = "v0=" + hmac.new(SLACK_SIGNING_SECRET.encode(), sig_basestring.encode(), hashlib.sha256).hexdigest()
    slack_sig = req.headers.get("X-Slack-Signature","")
    return hmac.compare_digest(my_sig, slack_sig)

@app.route("/slack/interact", methods=["POST"])
def interact():
    if not verify(request):
        return abort(403)
    payload_raw = request.form.get("payload")
    if not payload_raw:
        return jsonify({"text":"no payload"}), 200
    payload = json.loads(payload_raw)
    user = payload.get("user",{}).get("username") or payload.get("user",{}).get("id")
    action = payload.get("actions",[{}])[0]
    value = action.get("value","")
    topic, variant = value.split("||") if "||" in value else (value, "")
    append_row("Approvals", [user, topic, variant])
    return jsonify({"response_type":"ephemeral","text":f"Recorded approval for {variant} on {topic}."})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",5000)), debug=True)
