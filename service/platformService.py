import psycopg2
from psycopg2 import sql

class PlatformService:
    def __init__(self, db_params):
        self.params = db_params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def add_platform(self, platform_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO Platform (Name)
                            VALUES (%s)
                            """)
            cursor.execute(query, (platform_name,))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding platform: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def get_platform(self, platform_name):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT PlatformID FROM Platform WHERE Name = %s
                            """)
            cursor.execute(query, (platform_name,))
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            print(f"Error fetching platform: {e}")
            return None
        finally:
            cursor.close()
            connection.close()

    def get_all_platforms(self):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT * FROM Platform
                            """)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            print(f"Error fetching platforms: {e}")
            return None
        finally:
            cursor.close()
            connection.close()