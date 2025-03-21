# register <username> <password> <first_name> <last_name> <email>
# login <username> <password>
# follow <email>
# unfollow <email>
# search_movies --title "Movie Name"
# search_movies --release_date YYYY-MM-DD
# search_movies --cast "Actor Name"
# search_movies --studio "Studio Name"
# search_movies --genre "Genre"
# sort_movies --by name|studio|genre|year --order asc|desc
# watch_movie <movie_id>
# rate_movie <movie_id> <rating>
# create_collection <collection_name>
# add_to_collection <collection_name> <movie_id>
# remove_from_collection <collection_name> <movie_id>
# list_collections
# rename_collection <old_name> <new_name>
# delete_collection <collection_name>
# list_following
# list_followers
# list_users
# list_movies

import argparse
import psycopg2
from sshtunnel import SSHTunnelForwarder
from service.userService import UserService
from service.movieService import MovieService
from service.collectionService import CollectionService

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

# Initialize services
userService = UserService(db_params)
movieService = MovieService(db_params)
collectionService = CollectionService(db_params)


def main():
    parser = argparse.ArgumentParser(description="Movie Database CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    register_parser = subparsers.add_parser("register", help="Register a new user")
    register_parser.add_argument("username", help="Username")
    register_parser.add_argument("password", help="Password")
    register_parser.add_argument("first_name", help="First name")
    register_parser.add_argument("last_name", help="Last name")
    register_parser.add_argument("email", help="Email")
    register_parser.set_defaults(func=lambda args: userService.register(args.username, args.password, args.first_name, args.last_name, args.email))

    login_parser = subparsers.add_parser("login", help="Login to the system")
    login_parser.add_argument("username", help="Username")
    login_parser.add_argument("password", help="Password")
    login_parser.set_defaults(func=lambda args: userService.login(args.username, args.password))

    follow_parser = subparsers.add_parser("follow", help="Follow a user")
    follow_parser.add_argument("email", help="Email of user to follow")
    follow_parser.set_defaults(func=lambda args: userService.follow(args.email))

    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)  # Automatically execute the function
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

"""
python3 cli.py search_movies --title "Inception"
python3 cli.py search_movies --release_date 2010-07-16
python3 cli.py search_movies --cast "Leonardo DiCaprio"
python3 cli.py search_movies --studio "Warner Bros"
python3 cli.py search_movies --genre "Sci-Fi"

(1, 'Economic election indicate', 157, 'R', datetime.date(2023, 4, 20))
(2, 'Foot bring', 119, 'X', datetime.date(2017, 7, 25))
(3, 'Human notice', 180, 'PG-13', datetime.date(2019, 5, 14))
(4, 'Left to bill', 119, 'NR', datetime.date(2015, 2, 24))
(5, 'Team attack fire goal', 124, 'G', datetime.date(2015, 4, 30))
(6, 'Thing weight', 80, 'PG', datetime.date(2017, 12, 27))
(7, 'War hit', 91, 'NR', datetime.date(2020, 12, 9))
(8, 'Rest fire', 161, 'G', datetime.date(2015, 10, 16))
(9, 'Among camera experience', 125, 'G', datetime.date(2017, 6, 2))
(10, 'Because left nor', 84, 'PG', datetime.date(2024, 5, 10))
"""
