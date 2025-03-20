import os
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager

class Connection:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "db")
        self.database = os.getenv("DB_NAME", "stock_marketdb")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "12345")
        self.port = os.getenv("DB_PORT", "5433")

    @contextmanager
    def connect(self):
        """Context manager for database connection handling"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            yield cursor
            conn.commit()  # Commit if no error
        except Exception as e:
            if conn:
                conn.rollback()  # Rollback in case of error
            print(f"❌ Database error: {e}")
        finally:
            if conn:
                cursor.close()
                conn.close()

    def execute(self, query, params=()):
        """Execute INSERT, UPDATE, DELETE queries"""
        with self.connect() as cursor:
            cursor.execute(query, params)

    def fetch(self, query, params=()):
        """Fetch SELECT queries"""
        with self.connect() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
