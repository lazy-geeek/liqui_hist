import mysql.connector
import os

# MySQL database configuration from environment variables
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}


# Test database connection
def test_db_connection():
    try:
        with mysql.connector.connect(**db_config) as conn:
            print("Database connection successful.")
    except Exception as e:
        print(f"Database connection failed: {e}")


test_db_connection()
