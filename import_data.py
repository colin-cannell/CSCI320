import psycopg2
from sshtunnel import SSHTunnelForwarder
from entities import movie
from entities import genre
from entities import platform
from entities import studio
from service import movieService
from load_data import *

username = "cjc1985"
password = "Calamity2023!"
db_name = "p32001_21"

db_params = {
    "host": "127.0.0.1",
    "database": db_name,
    "user": username,
    "password": password,
    "port": 40000  # Match SSH tunnel port
}

def connect_db():
    try:
        with SSHTunnelForwarder(
            ("starbug.cs.rit.edu", 22),
            user=username,
            password=password,  # Use SSH key if possible
            host="localhost",
            remote_bind_address=("localhost", 5432)) as server:
        
            server.start()
            print(f"SSH tunnel established. Local port: {server.local_bind_port}")
            params = {
                'dbname': db_name,
                'user': username,
                'password': password,
                'host': 'localhost',
                'port': server.local_bind_port
            }
            connection = psycopg2.connect(**params)
            curs = connection.cursor()
            print ("Connection established")

            return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

        
def execute_query(query):
    connection = connect_db()
    if not connection:
        return

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        
        if query.strip().upper().startswith("SELECT"):
            results = cursor.fetchall()
            for row in results:
                print(row)
        else:
            connection.commit()
            print("✅ Query executed successfully.")
    
    except psycopg2.Error as e:
        print(f"❌ Query error: {e}")
    
    finally:
        cursor.close()
        connection.close()

filepath = "movies_dataset.csv"
movies = load_movies_from_csv(filepath)[:10]

actior_dict = {}

# Create tables

M_id = 0
for movie in movies:
    M_id += 1
    query = f"""
    INSERT INTO movie (id, title, release_date, studio, genre, release_platform, length, mpaa_rating)
    VALUES ({M_id}, '{movie.title}', '{movie.release_date}', '{movie.studio}', '{movie.genre}', '{movie.release_platform}', {movie.length}, '{movie.mpaa_rating}');
    """
    print("creating movie")
    execute_query(query)
    for actor in movie.cast:
        query = f"""
        INSERT INTO movie_cast (movie_id, person_id)
        VALUES ({M_id}, '{actor}');
        """
        execute_query(query)
    print("creating movie cast")
    for director in movie.directors:
        query = f"""
        INSERT INTO movie_director (movie_id, director_name)
        VALUES ({M_id}, '{director}');
        """
        execute_query(query)
    print("creating movie director")



