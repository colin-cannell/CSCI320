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
    
    def list_movies(self):
        """List all movies in the database."""
        connection = self.connect_db()
        if not connection:
            return []

        try:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM Movie")
            for row in cursor.fetchall():
                print(row)
            return True
        except psycopg2.Error as e:
            print(f"Error listing movies: {e}")
            return []
        finally:
            cursor.close()
            connection.close()
        
    def search_movies(self, title=None, release_date=None, cast=None, studio=None, genre=None):
        """Search for movies based on various criteria and display relevant details."""
        connection = self.connect_db()
        if not connection:
            return []

        try:
            cursor = connection.cursor()
            
            query = """
            SELECT 
                m.MovieID,
                m.Name,
                m.Length,
                m.MpaaRating,
                m.ReleaseDate,
                COALESCE(directors.DirectorNames, 'N/A') AS Director,
                COALESCE(actors.CastMembers, 'N/A') AS Cast,
                COALESCE(NULLIF(user_ratings.AvgRating, NULL)::TEXT, 'N/A') AS AvgUserRating,
                COALESCE(studios.StudioNames, 'N/A') AS Studio
            FROM Movie m
            LEFT JOIN (
                SELECT md.MovieID, STRING_AGG(CONCAT(p.FirstName, ' ', p.LastName), ', ') AS DirectorNames
                FROM MovieDirector md
                JOIN Person p ON md.PersonID = p.PersonID
                GROUP BY md.MovieID
            ) AS directors ON m.MovieID = directors.MovieID
            LEFT JOIN (
                SELECT ma.MovieID, STRING_AGG(CONCAT(p.FirstName, ' ', p.LastName), ', ') AS CastMembers
                FROM MovieActor ma
                JOIN Person p ON ma.PersonID = p.PersonID
                GROUP BY ma.MovieID
            ) AS actors ON m.MovieID = actors.MovieID
            LEFT JOIN (
                SELECT r.MovieID, ROUND(AVG(r.Rating), 1) AS AvgRating
                FROM MovieRating r
                GROUP BY r.MovieID
            ) AS user_ratings ON m.MovieID = user_ratings.MovieID
            LEFT JOIN (
                SELECT ms.MovieID, STRING_AGG(s.Name, ', ') AS StudioNames
                FROM MovieStudio ms
                JOIN Studio s ON ms.StudioID = s.StudioID
                GROUP BY ms.MovieID
            ) AS studios ON m.MovieID = studios.MovieID
            WHERE 1=1
            """
            
            params = []

            if title:
                query += " AND m.Name ILIKE %s"
                params.append(f"%{title}%")
            if release_date:
                query += " AND m.ReleaseDate = %s"
                params.append(release_date)
            if cast:
                query += """
                AND m.MovieID IN (
                    SELECT ma.MovieID FROM MovieActor ma 
                    JOIN Person p ON ma.PersonID = p.PersonID 
                    WHERE CONCAT(p.FirstName, ' ', p.LastName) ILIKE %s
                )"""
                params.append(f"%{cast}%")
            if studio:
                query += """
                AND m.MovieID IN (
                    SELECT ms.MovieID FROM MovieStudio ms 
                    JOIN Studio s ON ms.StudioID = s.StudioID 
                    WHERE s.Name ILIKE %s
                )"""
                params.append(f"%{studio}%")
            if genre:
                query += """
                AND m.MovieID IN (
                    SELECT mg.MovieID FROM MovieGenre mg 
                    JOIN Genre g ON mg.GenreID = g.GenreID 
                    WHERE g.Name ILIKE %s
                )"""
                params.append(f"%{genre}%")

            cursor.execute(query, params)
            results = cursor.fetchall()

            for row in sorted(results, key=lambda x: (x[1], x[4])):  # Sorting by Title (index 1) and ReleaseDate (index 4)
                print(f"MovieID: {row[0]}, Title: {row[1]}, Length: {row[2]} min, MPAA: {row[3]}, Release: {row[4]}, Director: {row[5]}, Cast: {row[6]}, Avg Rating: {row[7]}, Studio: {row[8]}")   

            return results
        except psycopg2.Error as e:
            print(f"Error searching movies: {e}")
            return []
        finally:
            cursor.close()
            connection.close()

    def search_by_title(self, title):
        """Search for movies by title."""
        return self.search_movies(title=title)
    
    def search_by_release_date(self, release_date):
        """Search for movies by release date."""
        return self.search_movies(release_date=release_date)
    
    def search_by_cast(self, cast):
        """Search for movies by cast."""
        return self.search_movies(cast=cast)
    
    def search_by_studio(self, studio):
        """Search for movies by studio."""
        return self.search_movies(studio=studio)
    
    def search_by_genre(self, genre):
        """Search for movies by genre."""
        return self.search_movies(genre=genre)
    
    def add_movie(self, title, length, mpaa_rating, release_date):
        """Insert a movie into the Movie table."""
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO Movie (Name, Length, MpaaRating, ReleaseDate) 
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
            return cursor.fetchone()
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
        
    def get_movie_id_by_title(self, title):
            connection = self.connect_db()
            if not connection:
                return None

            try:
                cursor = connection.cursor()
                query = "SELECT MovieID FROM Movie WHERE Name = %s"
                cursor.execute(query, (title,))
                return cursor.fetchone()
            except psycopg2.Error as e:
                print(f"Error fetching movie ID: {e}")
                return None
            finally:
                cursor.close()
                connection.close()

    def watch_movie(self, movie_id, user_id):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO watchHistory (MovieID, userID) 
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id,user_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error watching movie: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def rate_movie(self, userid, movie_id, rating):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = """
                INSERT INTO MovieRating (MovieID, userid, Rating) 
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """
            cursor.execute(query, (movie_id, userid,  rating))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error rating movie: {e}")
            return False
        finally:
            cursor.close()
            connection.close()