import psycopg2
from psycopg2 import sql

class GenreService:
    def __init__(self, db_params):
        self.params = db_params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def add_genre(self, genre_name):
        if isinstance(genre_name, list):
            genre_name = genre_name[0]  # Get the first item in the list if it's a list
        
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO Genre (Name)
                            VALUES (%s)
                            """)
            cursor.execute(query, (genre_name,))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding genre: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def get_genre(self, genre_name):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT GenreID FROM Genre WHERE Name = %s
                            """)
            cursor.execute(query, (genre_name,))
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            print(f"Error fetching genre: {e}")
            return None
        finally:
            cursor.close()
            connection.close()

    def get_all_genres(self):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT * FROM Genre
                            """)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            print(f"Error fetching genres: {e}")
            return None
        finally:
            cursor.close()
            connection.close()