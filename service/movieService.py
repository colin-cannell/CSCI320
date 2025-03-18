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

    def search_movies(self, title=None, release_date=None, cast=None, studio=None, genre=None):
        connection = self.connect_db()
        if not connection:
            return []

        try:
            cursor = connection.cursor()
            base_query = """
                            SELECT id, title, release_date, studio, genre FROM movies WHERE 
                        """
            conditions = []
            params = []

            if title:
                conditions.append("title ILIKE %s")
                params.append(f"%{title}%")
            if release_date:
                conditions.append("release_date = %s")
                params.append(release_date)
            if cast:
                conditions.append("id IN (SELECT movie_id FROM movie_cast WHERE actor_name ILIKE %s)")
                params.append(f"%{cast}%")
            if studio:
                conditions.append("studio ILIKE %s")
                params.append(f"%{studio}%")
            if genre:
                conditions.append("genre ILIKE %s")
                params.append(f"%{genre}%")

            query = base_query + " AND ".join(conditions) if conditions else "SELECT id, title, release_date, studio, genre FROM movies"
            cursor.execute(query, params)
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error searching movies: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def sort_movies(self, sort_by, order):
        connection = self.connect_db()
        if not connection:
            return []

        try:
            cursor = connection.cursor()
            valid_columns = {"name": "title", "studio": "studio", "genre": "genre", "year": "release_date"}
            if sort_by not in valid_columns or order not in ["asc", "desc"]:
                print("Invalid sorting criteria")
                return []

            query = sql.SQL("""
                            SELECT id, title, release_date, studio, genre FROM movies ORDER BY {} {}
                            """).format(sql.Identifier(valid_columns[sort_by]), sql.SQL(order))
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error sorting movies: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def watch_movie(self, movie_id):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO watched_movies (movie_id) VALUES (%s)
                            """)
            cursor.execute(query, (movie_id,))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error marking movie as watched: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def rate_movie(self, movie_id, rating):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO movie_ratings (movie_id, rating) 
                            VALUES (%s, %s) ON CONFLICT (movie_id) 
                            DO UPDATE SET rating = EXCLUDED.rating
                            """)
            cursor.execute(query, (movie_id, rating))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error rating movie: {e}")
            return False
        finally:
            cursor.close()
            connection.close()