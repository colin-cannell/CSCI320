from datetime import datetime
from entities.person import Person
from entities.studio import Studio
from entities.platform import Platform
from entities.genre import Genre

class Movie:
    def __init__(self, title, genre, release_platform, release_date, length, cast, directors, studio, mpaa_rating):
        self.title = title
        self.genre = genre
        self.release_platform = release_platform
        self.release_date = release_date
        self.length = length
        self.cast = cast  # Make sure this is included in the constructor
        self.directors = directors
        self.studio = studio
        self.mpaa_rating = mpaa_rating

    def __str__(self):
        return f"Movie({self.title}, {self.release_date}, {self.studio}, {self.genre}, {self.release_platform}, {self.length}, {self.mpaa_rating}, {self.cast}, {self.directors})"
        
    def get_release_date(self):
        return self.release_date.strftime("%B %d, %Y")
    
    def get_release_platform(self):
        return str(self.release_platform)
    
    def get_genre(self):
        return str(self.genre)
    
    def get_studio(self):
        return str(self.studio)
    
    def get_director(self):
        return str(self.director)
    
    def get_cast(self):
        return [str(person) for person in self.cast]
    
    def get_length(self):
        return self.length
    
    def get_mpaa_rating(self):
        return self.mpaa_rating
    
    def get_id(self):
        return self.movie_id
    
    def get_title(self):
        return self.title
    
    def get_top_movies_by_user(self, user_id, limit=10):
        query = """
            SELECT m.title, wm.rating, wm.plays
            FROM watched_movies wm
            JOIN movies m ON wm.movie_id = m.id
            WHERE wm.user_id = %s
            ORDER BY wm.rating DESC, wm.plays DESC
            LIMIT %s
        """
        self.cursor.execute(query, (user_id, limit))
        return self.cursor.fetchall()
        

    
