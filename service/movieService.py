import psycopg2
from psycopg2 import sql

class MovieService:
    def __init__(self, db_params):
        self.db_params = db_params

    def connect_db(self):
        try:
            return psycopg2.connect(**self.db_params)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None

    def add_movie(self, title, length, mpaa_rating, release_date):
        """Insert a movie into the Movie table."""
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO Movie (Name, Length, MpaaRation, ReleaseDate) 
                VALUES (%s, %s, %s, %s) RETURNING MovieID
            """
            cursor.execute(query, (title, length, mpaa_rating, release_date))
            movie_id = cursor.fetchone()[0]  # Get the generated MovieID
            connection.commit()
            return movie_id
        except psycopg2.Error as e:
            print(f"Error adding movie: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def get_movie(self, title):
        """Retrieve a movie by title."""
        connection = self.connect_db()
        if not connection:
            return None

        try:
            cursor = connection.cursor()
            query = "SELECT MovieID FROM Movie WHERE Name = %s"
            cursor.execute(query, (title,))
            return cursor.fetchone()  # Fetch one movie, not all
        except psycopg2.Error as e:
            print(f"Error fetching movie: {e}")
            return None
        finally:
            cursor.close()
            connection.close()

    def add_movie_genre(self, movie_id, genre_id):
        """Link a movie to a genre."""
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO MovieGenre (MovieID, GenreID) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id, genre_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie genre: {e}")
            return False
        

    def add_movie_studio(self, movie_id, studio_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO MovieStudio (MovieID, StudioID) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id, studio_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie studio: {e}")
            return False

    def add_movie_platform(self, movie_id, platform_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO MoviePlatform (MovieID, PlatformID) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id, platform_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie platform: {e}")
            return False
        

    def add_movie_director(self, movie_id, person_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO MovieDirector (MovieID, PersonID) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id, person_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie director: {e}")
            return False
        
       
    def add_movie_actor(self, movie_id, person_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO MovieActor (MovieID, PersonID) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id, person_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error adding movie actor: {e}")
            return False
        
        
    