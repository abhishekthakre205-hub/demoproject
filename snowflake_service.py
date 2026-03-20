# snowflake_service.py

import os
import random
import string
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------------------
# Load .env explicitly
# ------------------------------------------------------------------
dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)

# ------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# Optional but strongly recommended
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")


# ------------------------------------------------------------------
# Connection helper
# ------------------------------------------------------------------
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        # account=SNOWFLAKE_ACCOUNT,
        account=SNOWFLAKE_ACCOUNT,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )


# ------------------------------------------------------------------
# Password generator
# ------------------------------------------------------------------
def generate_password(length: int = 16) -> str:
    """
    Generate a strong password for Snowflake:
    - At least 1 uppercase
    - At least 1 lowercase
    - At least 1 digit
    - No special characters
    """
    while True:
        password = "".join(
            random.choices(string.ascii_letters + string.digits, k=length)
        )
        if (
            any(c.isupper() for c in password)
            and any(c.islower() for c in password)
            and any(c.isdigit() for c in password)
        ):
            return password


# ------------------------------------------------------------------
# Create user
# ------------------------------------------------------------------
def onboard_user(username: str, role: str):
    import secrets

    # ✅ Strong password with special characters
    temp_password = secrets.token_urlsafe(12) + "@A1"

    print("Generated password:", temp_password)  # debug

    conn = get_snowflake_connection()
    cs = conn.cursor()

    try:
        query = f'''
        CREATE USER "{username.upper()}"
        LOGIN_NAME = "{username.upper()}"
        PASSWORD = '{temp_password}'
        DEFAULT_ROLE = "{role.upper()}"
        MUST_CHANGE_PASSWORD = TRUE
        '''

        cs.execute(query)
        conn.commit()

        return temp_password

    except snowflake.connector.errors.ProgrammingError as e:
        print("Snowflake error while creating user:", e)
        return None

    finally:
        cs.close()
        conn.close()


# ------------------------------------------------------------------
# Reset user password
# ------------------------------------------------------------------
def reset_password(username: str):
    """
    Resets password for an existing Snowflake user.
    Returns the new password or None on failure.
    """
    new_password = generate_password()

    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        cs.execute(
            f"""
            ALTER USER "{username.upper()}"
            SET PASSWORD = '{new_password}'
            """
        )
        conn.commit()
        return new_password

    except snowflake.connector.errors.ProgrammingError as e:
        print("Snowflake error while resetting password:", e)
        return None

    finally:
        cs.close()
        conn.close()

def delete_user(username: str):
    """
    Deletes an existing Snowflake user.
    Returns True on success, False on failure.
    """
    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        cs.execute(
            f"""
            DROP USER IF EXISTS "{username.upper()}"
            """
        )
        conn.commit()
        return True

    except snowflake.connector.errors.ProgrammingError as e:
        print("Snowflake error while deleting user:", e)
        return False

    finally:
        cs.close()
        conn.close()