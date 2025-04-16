# register user : username password first_name last_name email
# login : username password

# watch movie : movieid userid
# rate movie : userid movieid rating

import psycopg2
from psycopg2 import sql

import random

from service.userService import UserService
from service.movieService import MovieService

NUM_USERS = 100
# already registered 100 users
NUM_MOVIES = 25 # watch and rate 25 movies per user

register_path = "register.txt"

db_params = {
    "host": "127.0.0.1",
    "database": "p32001_21",
    "user": "cjc1985",
    "password": "Calamity2023!",
    "port": 40000  # Match SSH tunnel port
}

def connect_db(params):
        try:
            conn = psycopg2.connect(**params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None

def make_login_file():
    with open("login.txt", 'w') as login:
        with open(register_path, 'r') as register:
            lines = register.readlines()
            for line in lines:
                info = line.strip().split()
                username = info[0]
                password = info[1]
                login.write(f"{username} {password}\n")     

def get_movie_id(userid):
    try :
        conn = connect_db(db_params)
        if conn is None:
            print("Connection to database failed.")
            return None
        
        cursor = conn.cursor()

        query = sql.SQL("""
                        SELECT MovieID FROM Movie
                        """)
        cursor.execute(query)
        movieids = cursor.fetchall()

        query = sql.SQL("""
                        select movieid from movierating 
                        where userid = %s
                        """)
        cursor.execute(query, (userid,))
        userRatings = cursor.fetchall()
        cursor.close()
        
        movieid = random.choice(movieids)
        while (movieid,) in userRatings:  # Ensure the movie hasn't been rated by this user
            movieid = random.choice(movieids)[0]

        # Close the connection
        conn.close()
        return movieid[0]
    except psycopg2.Error as e:
        print(f"Error fetching movie IDs: {e}")
        return None

def generate_rating(userid, movieid):
    try:
        conn = connect_db(db_params)
        if conn is None:
            print("Connection to database failed.")
            return None
        
        cursor = conn.cursor()

        query = sql.SQL("""
                        select genreid from moviegenre
                        where movieid = %s
                        """)
        cursor.execute(query, (movieid,))
        genre = cursor.fetchall()


        # join movierating and movie tables together to get all ratings for movies of the same genre for the same user
        query = sql.SQL("""
                        SELECT mr.movieid, mr.rating
                        FROM movierating mr
                        JOIN moviegenre mg ON mr.movieid = mg.movieid
                        WHERE mg.genreid IN (SELECT genreid FROM moviegenre WHERE movieid = %s) AND mr.userid = %s;
                        """)
        cursor.execute(query, (movieid, userid))
        userRatings = cursor.fetchall()
        
        if not userRatings:
            rating = random.randint(1, 10)  # Random rating between 1 and 10 if no similar movies rated
        else:
            modifier = random.randint(-2, 2)  # Random modifier to add variability
            # Calculate the average rating of similar movies
            mean_rating = sum(rating for _, rating in userRatings) / len(userRatings)
            # Adjust the rating based on the mean rating and the modifier
            rating = mean_rating + modifier
            # Ensure the rating is within the valid range
            rating = max(1, min(10, round(rating)))  # Round to the nearest integer and clamp between 1 and 10
    
        cursor.close()
        conn.close()
        return rating
    except psycopg2.Error as e:
        print(f"Error generating rating: {e}")
        return None

def run_commands():
    with open(register_path, 'r') as register:
        lines = register.readlines()
        lines = lines[:NUM_USERS]  # Limit to NUM_USERS users
        if not lines:
            print("No users to process.")
            return

        # Connect to the database
        conn = connect_db(db_params)
        if conn is None:
            print("Failed to connect to the database.")
            return
        
        userService = UserService(db_params)
        movieService = MovieService(db_params)

        for line in lines:
            info = line.strip().split()
            if len(info) < 5:
                print(f"Skipping invalid line: {line.strip()}")
                continue
            
            username = info[0]
            password = info[1]
            first = info[2]
            last = info[3]
            email = info[4]

            # register user 
            userService.register(username, password, first, last, email)

            # get user id
            userid = userService.get_user_id(username)

            for _ in range(NUM_MOVIES):
                if userid is None:
                    print(f"Failed to retrieve user ID for {username}. Skipping to next user.")
                    
            
                # find new movie to watch
                movieid = get_movie_id(userid)

                # watch movie
                if movieid is not None:
                    print(f"User {username} is watching movie ID {movieid}.")
                    movieService.watch_movie(movieid, userid)

                    # rate movie
                    rating = generate_rating(userid, movieid)
                    if rating is not None:
                        print(f"User {username} is rating movie ID {movieid} with a rating of {rating}.")
                        movieService.rate_movie(userid, movieid, rating)
                    else:
                        print(f"Failed to generate a rating for user {username} and movie ID {movieid}.")
                else:
                    print(f"No available movie for user {username}.")

        # Close the database connection
        conn.close()

if __name__ == "__main__":
    # Run the commands to register users, watch movies, and rate them
    run_commands()

    print("All users processed successfully.")