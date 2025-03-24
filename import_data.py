import psycopg2
from load_data import *
from service.personService import PersonService
from service.studioService import StudioService
from service.movieService import MovieService
from service.genreService import GenreService
from service.platformService import PlatformService

# Database connection parameters
db_params = {
    "host": "127.0.0.1",
    "database": "p32001_21",
    "user": "cjc1985",
    "password": "Calamity2023!",
    "port": 40000
}

# Load movie data
filepath = "movies_10000.csv"
movies = load_movies_from_csv(filepath)

# Service instances
personService = PersonService(db_params)
studioService = StudioService(db_params)
movieService = MovieService(db_params)
genreService = GenreService(db_params)
platformService = PlatformService(db_params)

# Caching to avoid redundant DB lookups
person_cache = {}
genre_cache = {}
platform_cache = {}
studio_cache = {}

last_movie_loaded = 121
batch_size = 50  # Adjust batch size for efficiency
movie_data = []

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

try:
    for index, movie in enumerate(movies[last_movie_loaded:], start=last_movie_loaded + 1):
        # Ensure actors exist
        actor_ids = []
        for actor in movie.cast:
            if actor not in person_cache:
                first, last = actor.split(" ")[0], actor.split(" ")[-1]
                person = personService.get_person(first, last)
                if not person:
                    personService.add_person(first, last)
                    person = personService.get_person(first, last)
                person_cache[actor] = person[0] if person else None
            if person_cache[actor]:
                actor_ids.append(person_cache[actor])

        # Ensure directors exist
        director_ids = []
        for director in movie.directors:
            if director not in person_cache:
                first, last = director.split(" ")[0], director.split(" ")[-1]
                person = personService.get_person(first, last)
                if not person:
                    personService.add_person(first, last)
                    person = personService.get_person(first, last)
                person_cache[director] = person[0] if person else None
            if person_cache[director]:
                director_ids.append(person_cache[director])

        # Ensure genre exists
        if movie.genre not in genre_cache:
            genre_obj = genreService.get_genre(movie.genre)
            if not genre_obj:
                genreService.add_genre(movie.genre)
                genre_obj = genreService.get_genre(movie.genre)
            genre_cache[movie.genre] = genre_obj[0] if genre_obj else None
        genre_id = genre_cache[movie.genre]

        # Ensure platform exists
        if movie.release_platform not in platform_cache:
            platform_obj = platformService.get_platform(movie.release_platform)
            if not platform_obj:
                platformService.add_platform(movie.release_platform)
                platform_obj = platformService.get_platform(movie.release_platform)
            platform_cache[movie.release_platform] = platform_obj[0] if platform_obj else None
        platform_id = platform_cache[movie.release_platform]

        # Ensure studio exists
        if movie.studio not in studio_cache:
            studio_obj = studioService.get_studio(movie.studio)
            if not studio_obj:
                studioService.add_studio(movie.studio)
                studio_obj = studioService.get_studio(movie.studio)
            studio_cache[movie.studio] = studio_obj[0] if studio_obj else None
        studio_id = studio_cache[movie.studio]

        # Check if movie already exists
        movie_obj = movieService.get_movie(movie.title)
        if not movie_obj:
            movie_id = movieService.add_movie(movie.title, movie.length, movie.mpaa_rating, movie.release_date)
        else:
            movie_id = movie_obj[0]

        # Store batched insert data
        if genre_id:
            movie_data.append((movie_id, genre_id, "genre"))
        if platform_id:
            movie_data.append((movie_id, platform_id, "platform"))
        if studio_id:
            movie_data.append((movie_id, studio_id, "studio"))
        for director_id in director_ids:
            movie_data.append((movie_id, director_id, "director"))
        for actor_id in actor_ids:
            movie_data.append((movie_id, actor_id, "actor"))

        # Bulk insert every `batch_size` movies
        if len(movie_data) >= batch_size:
            cursor.executemany("""
                INSERT INTO movie_links (movie_id, entity_id, entity_type) 
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """, movie_data)
            conn.commit()
            movie_data.clear()

        print(f"Processed movie {index}", end="\r", flush=True)

    # Final batch insert
    if movie_data:
        cursor.executemany("""
            INSERT INTO movie_links (movie_id, entity_id, entity_type) 
            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
        """, movie_data)
        conn.commit()

    print("✅ Data import completed successfully.")

except Exception as e:
    print(f"❌ Error during import: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()