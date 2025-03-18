import psycopg2
from psycopg2 import sql

class CollectionService:
    def __init__(self, db_params):
        self.db_params = db_params

    def connect_db(self):
        try:
            return psycopg2.connect(**self.db_params)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None

    def create_collection(self, user_id, collection_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO collections (user_id, name) 
                            VALUES (%s, %s)
                            """)
            cursor.execute(query, (user_id, collection_name))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error creating collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def add_to_collection(self, user_id, collection_name, movie_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                INSERT INTO collection_movies (collection_id, movie_id)
                VALUES ((SELECT id FROM collections WHERE user_id = %s AND name = %s), %s)
            """)
            cursor.execute(query, (user_id, collection_name, movie_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie to collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def remove_from_collection(self, user_id, collection_name, movie_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                DELETE FROM collection_movies 
                WHERE collection_id = (SELECT id FROM collections WHERE user_id = %s AND name = %s)
                AND movie_id = %s
                """)
            cursor.execute(query, (user_id, collection_name, movie_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error removing movie from collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def list_collections(self, user_id):
        connection = self.connect_db()
        if not connection:
            return []
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT name FROM collections WHERE user_id = %s
                            """)
            cursor.execute(query, (user_id,))
            return [row[0] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"Error listing collections: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def rename_collection(self, user_id, old_name, new_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            UPDATE collections SET name = %s WHERE user_id = %s AND name = %s
                            """)
            cursor.execute(query, (new_name, user_id, old_name))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error renaming collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def delete_collection(self, user_id, collection_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            DELETE FROM collections WHERE user_id = %s AND name = %s
                            """)
            cursor.execute(query, (user_id, collection_name))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error deleting collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()