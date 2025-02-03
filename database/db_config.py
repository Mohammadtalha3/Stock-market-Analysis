import psycopg2

class Connection:
    def __init__(self):
        self.host = 'localhost'
        self.database = 'stock_market'
        self.user = 'postgres'
        self.password = '12345'
        self.connection = None  # Initialize connection as None
        self.cursor = None  # Initialize cursor as None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("✅ Connected to the database")
        except Exception as e:
            print(f"❌ Failed to connect to the database: {e}")

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("🔒 Database connection closed.")

    def commit(self):
        if self.connection:
            self.connection.commit()

    def execute(self, query, params):
        if not self.connection:
            raise Exception("⚠️ Database connection is not established. Call `connect()` first.")
        try:
            self.cursor.execute(query, params)
            self.commit()
        except Exception as e:
            print(f"❌ Error executing query: {e}")

    def fetch(self, query):
        if not self.connection:
            raise Exception("⚠️ Database connection is not established. Call `connect()` first.")
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            return None

