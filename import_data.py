import psycopg2
from sshtunnel import SSHTunnelForwarder
from entities import movie
from load_data import *
from service.personService import PersonService
from service.studioService import StudioService
from service.movieService import MovieService
from service.genreService import GenreService
from service.platformService import PlatformService

# Import modules for handling business logic
db_params = {
    "host": "127.0.0.1",
    "database": "p32001_21",
    "user": "cjc1985",
    "password": "Calamity2023!",
    "port": 40000  # Match SSH tunnel port
}

username = "cjc1985"
password = "Calamity2023!"
db_name = "p32001_21"

def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None

def execute_query(query, params=None, fetch=False):
    """Executes a SQL query with optional parameters and returns fetched results if needed."""
    connection = connect_db()
    if not connection:
        return None

    try:
        cursor = connection.cursor()
        cursor.execute(query, params)

        if fetch:
            result = cursor.fetchall()
        else:
            connection.commit()
            result = None

        cursor.close()
        connection.close()
        return result
    except psycopg2.Error as e:
        print(f"❌ Query error: {e}")
        return None

# Load movie data
filepath = "movies_dataset.csv"
movies = load_movies_from_csv(filepath)

personService = PersonService(db_params)
studioService = StudioService(db_params)
movieService = MovieService(db_params)
genreService = GenreService(db_params)
platformService = PlatformService(db_params)



for movie in movies:
    # Ensure all actors exist in the database
    actor_ids = []
    for actor in movie.cast:
        if len(actor.split(" ")) > 2:
            first, last = actor.split(" ")[0], actor.split(" ")[-1]
        else:
            first, last = actor.split(" ")

        person = personService.get_person(first, last)
        if not person:  # If the person is not found, add them first
            personService.add_person(first, last)
            person = personService.get_person(first, last)
        
        if person:  # Ensure person is valid before accessing index 0
            actor_ids.append(person[0])
        else:
            print(f"Error: Could not retrieve or add person '{first} {last}'")


    # Ensure all directors exist in the database
    director_ids = []
    for director in movie.directors:
        if len(director.split(" ")) > 2:
            first, last = director.split(" ")[0], director.split(" ")[-1]
        else:
            first, last = director.split(" ")
        person = personService.get_person(first, last)
        if person is None:  # If the person is not found, add them first
            personService.add_person(first, last)
            person = personService.get_person(first, last)
        
        if person:  # Ensure person is valid before accessing index 0
            director_ids.append(person[0])
        else:
            print(f"Error: Could not retrieve or add person '{first} {last}'")

    # Ensure all genres exist
    genre_ids = []
    genre_obj = genreService.get_genre(movie.genre)
    if not genre_obj:
        genreService.add_genre(movie.genre)
        genre_obj = genreService.get_genre(movie.genre)
    
    if genre_obj:
        genre_ids.append(genre_obj[0])
    else:
        print(f"Error: Could not retrieve or add genre '{movie.genre}'")

    # Ensure all platforms exist
    platform_ids = []
    platform_obj = platformService.get_platform(movie.release_platform)
    if not platform_obj:
        platformService.add_platform(movie.release_platform)
        platform_obj = platformService.get_platform(movie.release_platform)
    
    if platform_obj:
        platform_ids.append(platform_obj[0])
    else:
        print(f"Error: Could not retrieve or add platform '{movie.release_platform}'")

    # Ensure all studios exist
    studio_ids = []
    studio_obj = studioService.get_studio(movie.studio)
    if not studio_obj:
        studioService.add_studio(movie.studio)
        studio_obj = studioService.get_studio(movie.studio)
    
    if studio_obj:
        studio_ids.append(studio_obj[0])
    else:
        print(f"Error: Could not retrieve or add studio '{movie.studio}'")

    # Add the movie if it doesn't exist
    movie_obj = movieService.get_movie(movie.title)
    if not movie_obj:
        movie_id = movieService.add_movie(movie.title, movie.length, movie.mpaa_rating, movie.release_date)
    else:
        movie_id = movie_obj[0]
    
    # Link movie to genres
    for genre_id in genre_ids:
        movieService.add_movie_genre(movie_id, genre_id)

    # Link movie to platforms
    for platform_id in platform_ids:
        movieService.add_movie_platform(movie_id, platform_id)

    # Link movie to studios
    for studio_id in studio_ids:
        studio_id = int(studio_id[0])
        movieService.add_movie_studio(movie_id, studio_id)

    # Link movie to directors
    for director_id in director_ids:
        movieService.add_movie_director(movie_id, director_id)

    # Link movie to actors
    for actor_id in actor_ids:
        movieService.add_movie_actor(movie_id, actor_id)

print("Data import completed successfully.")