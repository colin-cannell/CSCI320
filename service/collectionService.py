import psycopg2
from psycopg2 import sql
from service.movieService import MovieService

class CollectionService:
    def __init__(self, db_params):
        self.db_params = db_params
        self.movie_service = MovieService(db_params)

    def connect_db(self):
        try:
            return psycopg2.connect(**self.db_params)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None

    def create_collection(self, collection_id,  user_id, collection_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO collection (collectionid, userid, Name) 
                            VALUES (%s, %s, %s)
                            """)
            cursor.execute(query, (collection_id, user_id, collection_name))
            connection.commit()
            print("Collection created successfully.")
            return True
        except psycopg2.Error as e:
            print(f"Error creating collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def add_to_collection(self, user_id, collection_name, movieid):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Check if the collection exists for the given user_id and collection_name
            query = sql.SQL("""
                SELECT CollectionID 
                FROM Collection 
                WHERE UserID = %s AND Name = %s
            """)
            cursor.execute(query, (user_id, collection_name))
            collectionid = cursor.fetchone()
            
            # If no collection is found, print an error and return False
            if not collectionid:
                print(f"Error: Collection '{collection_name}' not found for user {user_id}.")
                return False
            
            collectionid = collectionid[0]  # Extract the CollectionID
            
            # Insert the movie into the CollectionMovie table
            query = sql.SQL("""
                INSERT INTO CollectionMovie (CollectionID, MovieID)
                VALUES (%s, %s)
            """)
            cursor.execute(query, (collectionid, movieid))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie to collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def remove_from_collection(self, collection_id, movie_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:            
            cursor = connection.cursor()
            # Now remove the movie from the collection
            query = sql.SQL("""
                DELETE FROM CollectionMovie 
                WHERE CollectionID = %s AND MovieID = %s
            """)
            cursor.execute(query, (collection_id, movie_id))
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
                            SELECT name, totalmovies, totallength FROM collection WHERE userid = %s
                            """)
            cursor.execute(query, (user_id,))
            collections = cursor.fetchall()
            if not collections:
                print("No collections found.")
                return False
            for collection in sorted(collections):
                print(collection)
            print("Collections listed successfully.")
            return True

        except psycopg2.Error as e:
            print(f"Error listing collections: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def rename_collection(self, old_name, new_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            UPDATE collection SET name = %s WHERE name = %s
                            """)
            cursor.execute(query, (new_name, old_name))
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

    def watch_collection(self, collection_id, user_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT movieid FROM collectionmovie WHERE collectionid = %s
                            """)
            cursor.execute(query, (collection_id,))
            movies = cursor.fetchall()
            if not movies:
                print("No movies found in the collection.")
                return False
            
            for movie in movies:
                movie_id = movie[0]
                self.movie_service.watch_movie(movie_id, user_id)
            return True
        except psycopg2.Error as e:
            print(f"Error watching collection: {e}")
            return False
        finally:
            cursor.close()
            connection.close()