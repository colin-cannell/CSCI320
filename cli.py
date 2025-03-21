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
    "user": "wk3521",
    "password": "Robloxbabe27!",
    "port": 40000  # Match SSH tunnel port
}

username = "wk3521"
password = "Robloxbabe27!"
db_name = "p32001_21"

# Initialize services
userService = UserService(db_params)
movieService = MovieService(db_params)
collectionService = CollectionService(db_params)
# socialService = SocialService(db_params)


def main():
    parser = argparse.ArgumentParser(description="Movie Database CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Register command
    register_parser = subparsers.add_parser("register", help="Register a new user")
    register_parser.add_argument("username", help="Username")
    register_parser.add_argument("password", help="Password")
    register_parser.add_argument("first_name", help="First name")
    register_parser.add_argument("last_name", help="Last name")
    register_parser.add_argument("email", help="Email")

    get_creation_date_parser = subparsers.add_parser("get_creation_date", help="Get user creation date")
    get_creation_date_parser.add_argument("username", help="Username")

    get_last_login_date_parser = subparsers.add_parser("get_last_login", help="Get user last login date")
    get_last_login_date_parser.add_argument("username", help="Username")
    
    # Login command
    login_parser = subparsers.add_parser("login", help="Login to the system")
    login_parser.add_argument("username", help="Username")
    login_parser.add_argument("password", help="Password")
    
    # Follow command
    follow_parser = subparsers.add_parser("follow", help="Follow a user")
    follow_parser.add_argument("email", help="Email of user to follow")
    
    # Unfollow command
    unfollow_parser = subparsers.add_parser("unfollow", help="Unfollow a user")
    unfollow_parser.add_argument("email", help="Email of user to unfollow")
    
    # Search movies command
    search_parser = subparsers.add_parser("search_movies", help="Search for movies")
    search_group = search_parser.add_mutually_exclusive_group(required=True)
    search_group.add_argument("--title", help="Search by movie title")
    search_group.add_argument("--release_date", help="Search by release date (YYYY-MM-DD)")
    search_group.add_argument("--cast", help="Search by actor name")
    search_group.add_argument("--studio", help="Search by studio name")
    search_group.add_argument("--genre", help="Search by genre")
    
    # Sort movies command
    sort_parser = subparsers.add_parser("sort_movies", help="Sort movies")
    sort_parser.add_argument("--by", choices=["name", "studio", "genre", "year"], required=True, help="Field to sort by")
    sort_parser.add_argument("--order", choices=["asc", "desc"], required=True, help="Sort order")
    
    # Watch movie command
    watch_parser = subparsers.add_parser("watch_movie", help="Watch a movie")
    watch_parser.add_argument("userid", help="user id")
    watch_parser.add_argument("movie_id", help="ID of movie to watch")
    
    # Rate movie command
    rate_parser = subparsers.add_parser("rate_movie", help="Rate a movie")
    rate_parser.add_argument("userid", help="User ID")
    rate_parser.add_argument("movie_id", help="ID of movie to rate")
    rate_parser.add_argument("rating", type=float, help="Rating (0-5)")
    
    # Collection commands
    create_col_parser = subparsers.add_parser("create_collection", help="Create a new collection")
    create_col_parser.add_argument("collectionid", help="collection id")
    create_col_parser.add_argument("userid", help="user id")
    create_col_parser.add_argument("collection_name", help="Name of the collection")
    
    add_col_parser = subparsers.add_parser("add_to_collection", help="Add movie to collection")
    add_col_parser.add_argument("collection_name", help="Name of the collection")
    add_col_parser.add_argument("movie_id", help="ID of movie to add")
    
    remove_col_parser = subparsers.add_parser("remove_from_collection", help="Remove movie from collection")
    remove_col_parser.add_argument("collection_name", help="Name of the collection")
    remove_col_parser.add_argument("movie_id", help="ID of movie to remove")
    
    list_col_parser = subparsers.add_parser("list_collections", help="List all collections")
    list_col_parser.add_argument("userid", help="user id")
    
    rename_col_parser = subparsers.add_parser("rename_collection", help="Rename a collection")
    rename_col_parser.add_argument("old_name", help="Current collection name")
    rename_col_parser.add_argument("new_name", help="New collection name")
    
    delete_col_parser = subparsers.add_parser("delete_collection", help="Delete a collection")
    delete_col_parser.add_argument("collection_name", help="Name of collection to delete")

    watch_col_parser = subparsers.add_parser("watch_collection", help="Watch all movies in a collection")
    watch_col_parser.add_argument("userid", help="User ID")
    watch_col_parser.add_argument("collectionid", help="Collection ID")

    
    # Social commands
    subparsers.add_parser("list_following", help="List users you follow")
    subparsers.add_parser("list_followers", help="List your followers")
    subparsers.add_parser("list_users", help="List all users")
    
    # Movie listing command
    subparsers.add_parser("list_movies", help="List all movies")
    
    args = parser.parse_args()
    
    # Handle commands
    if args.command == "register":
        userService.register(args.username, args.password, args.first_name, args.last_name, args.email)
    elif args.command == "get_creation_date":
        userService.get_creation_date(args.username)
    elif args.command == "get_last_login":
        userService.get_last_login(args.username)
    elif args.command == "login":
        userService.login(args.username, args.password)
    elif args.command == "follow":
        socialService.follow(args.email)
    elif args.command == "unfollow":
        socialService.unfollow(args.email)
    elif args.command == "search_movies":
        if args.title:
            movieService.search_by_title(args.title)
        elif args.release_date:
            movieService.search_by_release_date(args.release_date)
        elif args.cast:
            movieService.search_by_cast(args.cast)
        elif args.studio:
            movieService.search_by_studio(args.studio)
        elif args.genre:
            movieService.search_by_genre(args.genre)
    elif args.command == "sort_movies":
        movieService.sort_movies(args.by, args.order)
    elif args.command == "watch_movie":
        userService.watch_movie(args.userid, args.movie_id)
    elif args.command == "rate_movie":
        userService.rate_movie(args.userid, args.movie_id, args.rating)
    elif args.command == "create_collection":
        collectionService.create_collection(args.collectionid, args.userid, args.collection_name)
    elif args.command == "add_to_collection":
        collectionService.add_to_collection(args.collection_name, args.movie_id)
    elif args.command == "remove_from_collection":
        collectionService.remove_from_collection(args.collection_name, args.movie_id)
    elif args.command == "list_collections":
        collectionService.list_collections(args.userid)
    elif args.command == "rename_collection":
        collectionService.rename_collection(args.old_name, args.new_name)
    elif args.command == "delete_collection":
        collectionService.delete_collection(args.collection_name)
    elif args.command == "watch_collection":
        userService.watch_collection(args.userid, args.collectionid)
    elif args.command == "list_following":
        socialService.list_following()
    elif args.command == "list_followers":
        socialService.list_followers()
    elif args.command == "list_users":
        userService.list_users()
    elif args.command == "list_movies":
        movieService.list_movies()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

