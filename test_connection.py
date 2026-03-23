# from dotenv import load_dotenv
# import os
# import snowflake.connector
# # Load .env file explicitly
# from pathlib import Path
# dotenv_path = Path(__file__).parent / ".env"
# load_dotenv(dotenv_path)

# print(os.getenv("SNOWFLAKE_USER"))  # Should print Megha777
# print(os.getenv("SNOWFLAKE_ACCOUNT"))  # Should print fcphibr/ti86772
# SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
# SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")


# conn = snowflake.connector.connect(
#     user=SNOWFLAKE_USER,
#     password=SNOWFLAKE_PASSWORD,
#     account=SNOWFLAKE_ACCOUNT,
   
# )

# cur = conn.cursor()
# cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
# print(cur.fetchone())
# cur.close()
# conn.close()


from dotenv import load_dotenv
import os
import snowflake.connector

# Load .env from project root
load_dotenv()

print("USER     =", os.getenv("SNOWFLAKE_USER"))
print("ACCOUNT  =", os.getenv("SNOWFLAKE_ACCOUNT"))
print("PASSWORD SET =", bool(os.getenv("SNOWFLAKE_PASSWORD")))

try:
    # conn = snowflake.connector.connect(
    #     user=os.getenv("SNOWFLAKE_USER"),
    #     password=os.getenv("SNOWFLAKE_PASSWORD"),
    #     account=os.getenv("SNOWFLAKE_ACCOUNT"),
    # )
    conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account="thvzgvn-mb22986"
)


    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
    print("CONNECTED:", cur.fetchone())

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Connection failed:")
    print(e)

