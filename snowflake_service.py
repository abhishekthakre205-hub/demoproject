# snowflake_service.py

import os
import random
import string
import secrets
from pathlib import Path

import snowflake.connector
from snowflake.connector.errors import ProgrammingError, OperationalError
from dotenv import load_dotenv

# ------------------------------------------------------------------
# Load .env explicitly for local development only
# ------------------------------------------------------------------
dotenv_path = Path(__file__).parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path)

# ------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")  # Just account locator + region
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# ------------------------------------------------------------------
# Snowflake connection helper
# ------------------------------------------------------------------
def get_snowflake_connection():
    """Return a Snowflake connection using env variables."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        return conn
    except OperationalError as e:
        print("Snowflake connection failed:", e)
        raise

# ------------------------------------------------------------------
# Strong password generator
# ------------------------------------------------------------------
def generate_password(length: int = 16) -> str:
    """
    Generate a strong password:
    - At least 1 uppercase
    - At least 1 lowercase
    - At least 1 digit
    - Optional special character added
    """
    while True:
        password = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        # Ensure it meets policy
        if (any(c.isupper() for c in password) and
            any(c.islower() for c in password) and
            any(c.isdigit() for c in password)):
            # Add a guaranteed special character at the end
            return password + "@A1"

# ------------------------------------------------------------------
# Create new Snowflake user
# ------------------------------------------------------------------
def onboard_user(username: str, role: str):
    """
    Create a new Snowflake user with a strong password.
    Returns the generated password on success, None on failure.
    """
    temp_password = secrets.token_urlsafe(12) + "@A1"
    print(f"[DEBUG] Generated password for {username}: {temp_password}")

    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        query = f"""
        CREATE USER "{username.upper()}"
        LOGIN_NAME = "{username.upper()}"
        PASSWORD = '{temp_password}'
        DEFAULT_ROLE = "{role.upper()}"
        MUST_CHANGE_PASSWORD = TRUE
        """
        cs.execute(query)
        conn.commit()
        return temp_password
    except ProgrammingError as e:
        print(f"[ERROR] Snowflake error creating user {username}: {e}")
        return None
    finally:
        cs.close()
        conn.close()

# ------------------------------------------------------------------
# Reset Snowflake user password
# ------------------------------------------------------------------
def reset_password(username: str):
    """
    Reset password for an existing Snowflake user.
    Returns the new password on success, None on failure.
    """
    new_password = generate_password()

    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        cs.execute(f"""
            ALTER USER "{username.upper()}"
            SET PASSWORD = '{new_password}'
        """)
        conn.commit()
        return new_password
    except ProgrammingError as e:
        print(f"[ERROR] Snowflake error resetting password for {username}: {e}")
        return None
    finally:
        cs.close()
        conn.close()

# ------------------------------------------------------------------
# Delete Snowflake user
# ------------------------------------------------------------------
def delete_user(username: str):
    """
    Deletes an existing Snowflake user.
    Returns True on success, False on failure.
    """
    conn = get_snowflake_connection()
    cs = conn.cursor()
    try:
        cs.execute(f'DROP USER IF EXISTS "{username.upper()}"')
        conn.commit()
        return True
    except ProgrammingError as e:
        print(f"[ERROR] Snowflake error deleting user {username}: {e}")
        return False
    finally:
        cs.close()
        conn.close()
