import hmac
import hashlib
import time
import os
from fastapi import HTTPException

from dotenv import load_dotenv
load_dotenv()

SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")

# Mapping Slack users to internal roles
USER_ROLES = {
    "U123456": "SUPPORT",
    "U234567": "ADMIN",
}

# Role → Allowed actions
ROLE_ACTIONS = {
    "SUPPORT": ["reset_password"],
    "ADMIN": ["onboard_user", "reset_password", "delete_user"],
}

def verify_slack_request(timestamp: str, body: str, slack_signature: str):
    # Prevent replay attacks (5 min)
    if abs(time.time() - int(timestamp)) > 60 * 5:
        raise HTTPException(status_code=403, detail="Request timestamp expired")

    basestring = f"v0:{timestamp}:{body}".encode("utf-8")
    my_signature = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode("utf-8"), basestring, hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(my_signature, slack_signature):
        raise HTTPException(status_code=403, detail="Invalid Slack signature")

def check_user_permission(slack_user_id: str, action: str):
    role = USER_ROLES.get(slack_user_id)
    if role is None or action not in ROLE_ACTIONS.get(role, []):
        raise HTTPException(
            status_code=403,
            detail=f"User {slack_user_id} is not authorized to perform {action}"
        )
