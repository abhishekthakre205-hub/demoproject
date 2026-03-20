from fastapi import FastAPI, Request, Form, Header, BackgroundTasks
from fastapi.responses import JSONResponse
from auth import verify_slack_request, check_user_permission
from snowflake_service import onboard_user, reset_password
import os
import requests

app = FastAPI()

VERIFY_SLACK = os.getenv("VERIFY_SLACK", "False").lower() == "true"

@app.get("/")
def health():
    return {"status": "running"}

@app.post("/slack/command")
async def slack_command(
    request: Request,
    background_tasks: BackgroundTasks,
    command: str = Form(...),
    text: str = Form(""),
    user_id: str = Form(...),
    response_url: str = Form(...),  # Needed for async reply
    x_slack_signature: str = Header(None),
    x_slack_request_timestamp: str = Header(None)
):
    # Slack verification
    if VERIFY_SLACK:
        if not x_slack_signature or not x_slack_request_timestamp:
            return JSONResponse({"text": "Slack headers missing"}, status_code=400)

        raw_body = await request.body()
        verify_slack_request(
            x_slack_request_timestamp,
            raw_body,
            x_slack_signature
        )

    # Validate command
    if command != "/snowflake":
        return {"text": "Unknown command"}

    args = text.split()
    if not args:
        return {
            "response_type": "ephemeral",
            "text": (
                "Usage:\n"
                "/snowflake onboard_user <username> <role>\n"
                "/snowflake reset_password <username>"
            )
        }

    action = args[0]

    # RBAC check
    if VERIFY_SLACK:
        check_user_permission(user_id, action)

    # Run Snowflake operation in background
    background_tasks.add_task(handle_snowflake_task, action, args, response_url)

    # Immediate Slack response
    return {
        "response_type": "ephemeral",
        "text": " Processing your request… You’ll get an update shortly."
    }

# ------------------- Background handler -------------------
def handle_snowflake_task(action, args, response_url):
    try:
        if action == "onboard_user" and len(args) == 3:
            username, role = args[1], args[2]
            password = onboard_user(username, role)
            if not password:
                message = f"Failed to onboard user *{username}*"
            else:
                message = (
                    f"User *{username}* onboarded with role *{role}*\n"
                    f" Temporary password: `{password}`"
                )

        elif action == "reset_password" and len(args) == 2:
            username = args[1]
            password = reset_password(username)
            if not password:
                message = f"Failed to reset password for *{username}*"
            else:
                message = (
                    f" Password reset for *{username}*\n"
                    f" New password: `{password}`"
                )

        elif action == "delete_user" and len(args) == 2:
            username = args[1]
            success = delete_user(username)
            if not success:
                message = f"Failed to delete user *{username}*"
            else:
                message = f"User *{username}* deleted successfully"
        else:
            message = "Invalid command format"

    except Exception as e:
        message = f"Error: {str(e)}"

    # Send final message to Slack
    requests.post(
        response_url,
        json={"response_type": "ephemeral", "text": message},
        timeout=5,
    )
