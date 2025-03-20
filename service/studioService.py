import psycopg2
from psycopg2 import sql

class StudioService:
    def __init__(self, db_params):
        self.params = db_params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def add_studio(self, studio_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO Studio (Name)
                            VALUES (%s)
                            """)
            cursor.execute(query, (studio_name,))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding studio: {e}")
            return False
        finally:
            cursor.close()
            connection.close()
    
    def get_studio(self, studio_name):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT * FROM Studio WHERE Name = %s
                            """)
            cursor.execute(query, (studio_name,))
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            print(f"Error fetching studio: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
