import shlex
import argparse
from service.userService import UserService
from service.movieService import MovieService
from service.collectionService import CollectionService
import os

# Import modules for handling business logic
db_params = {
    "host": "127.0.0.1",
    "database": "p32001_21",
    "user": os.getenv("DB_USER", ""), # export DB_USER="your_RIT_username"
    "password": os.getenv("DB_PASSWORD", ""), # export DB_PASSWORD="your_RIT_password"
    "port": 40000  # Match SSH tunnel port
}

username = os.getenv("DB_USER", "")
password = os.getenv("DB_PASSWORD", "")
db_name = "p32001_21"

# Initialize services
userService = UserService(db_params)
movieService = MovieService(db_params)
collectionService = CollectionService(db_params)
# socialService = SocialService(db_params)

def create_parser():
    parser = argparse.ArgumentParser(description="Movie Database CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # User-related commands
    register_parser = subparsers.add_parser("register", help="Register a new user")
    register_parser.add_argument("username")
    register_parser.add_argument("password")
    register_parser.add_argument("first_name")
    register_parser.add_argument("last_name")
    register_parser.add_argument("email")

    get_creation_date_parser = subparsers.add_parser("get_creation_date", help="Get user creation date")
    get_creation_date_parser.add_argument("username")

    get_last_login_parser = subparsers.add_parser("get_last_login", help="Get user last login date")
    get_last_login_parser.add_argument("username")

    get_creation_date_parser = subparsers.add_parser("get_creation_date", help="Get user creation date")
    get_creation_date_parser.add_argument("username", help="Username")

    get_last_login_date_parser = subparsers.add_parser("get_last_login", help="Get user last login date")
    get_last_login_date_parser.add_argument("username", help="Username")
    
    # Login command
    login_parser = subparsers.add_parser("login", help="Login to the system")
    login_parser.add_argument("username")
    login_parser.add_argument("password")

    follow_parser = subparsers.add_parser("follow", help="Follow a user")
    follow_parser.add_argument("follow")

    unfollow_parser = subparsers.add_parser("unfollow", help="Unfollow a user")
    unfollow_parser.add_argument("user")
    unfollow_parser.add_argument("unfollow")

    # Movie search and sorting
    search_parser = subparsers.add_parser("search_movies", help="Search for movies")
    search_group = search_parser.add_mutually_exclusive_group(required=True)
    search_group.add_argument("--title")
    search_group.add_argument("--release_date")
    search_group.add_argument("--cast")
    search_group.add_argument("--studio")
    search_group.add_argument("--genre")

    sort_parser = subparsers.add_parser("sort_movies", help="Sort movies")
    sort_parser.add_argument("--by", choices=["name", "studio", "genre", "year"], required=True)
    sort_parser.add_argument("--order", choices=["asc", "desc"], required=True)

    # Movie watching and rating
    watch_parser = subparsers.add_parser("watch_movie", help="Watch a movie")
    watch_parser.add_argument("movie_id")

    watch_collection_parser = subparsers.add_parser("watch_collection", help="Watch a collection of movies")
    watch_collection_parser.add_argument("collection_id")

    rate_parser = subparsers.add_parser("rate_movie", help="Rate a movie")
    rate_parser.add_argument("movie_id")
    rate_parser.add_argument("rating", type=float)

    # Collection commands
    create_col_parser = subparsers.add_parser("create_collection", help="Create a new collection")
    create_col_parser.add_argument("collectionid")
    create_col_parser.add_argument("collection_name")

    add_col_parser = subparsers.add_parser("add_to_collection", help="Add movie to collection")
    add_col_parser.add_argument("collectionid")
    add_col_parser.add_argument("movie_name")

    remove_col_parser = subparsers.add_parser("remove_from_collection", help="Remove movie from collection")
    remove_col_parser.add_argument("collection_id")
    remove_col_parser.add_argument("movie_id")

    list_col_parser = subparsers.add_parser("list_collections", help="List all collections")

    rename_col_parser = subparsers.add_parser("rename_collection", help="Rename a collection")
    rename_col_parser.add_argument("old_name")
    rename_col_parser.add_argument("new_name")

    delete_col_parser = subparsers.add_parser("delete_collection", help="Delete a collection")
    delete_col_parser.add_argument("collection_name")

    # Social commands
    subparsers.add_parser("list_following", help="List users you follow")
    subparsers.add_parser("list_followers", help="List your followers")
    subparsers.add_parser("list_users", help="List all users")

    # Movie listing command
    subparsers.add_parser("list_movies", help="List all movies")

    # Exit command
    subparsers.add_parser("exit", help="Exit the application")

    return parser

def main():
    parser = create_parser()  # Define parser once
    user_id = None

    while True:
        try:
            user_input = input("movie-db> ")  # Interactive prompt
            if not user_input.strip():
                continue  # Ignore empty input

            args = parser.parse_args(shlex.split(user_input))  # Parse user input

            # Handle commands
            if args.command == "register":
                userService.register(args.username, args.password, args.first_name, args.last_name, args.email)
            elif args.command == "get_creation_date":
                userService.get_creation_date(args.username)
            elif args.command == "get_last_login":
                userService.get_last_login(args.username)
            elif args.command == "login":
                userService.login(args.username, args.password)
                user_id = userService.get_user_id(args.username)
            elif args.command == "follow":
                userService.follow(user_id, args.follow)
            elif args.command == "unfollow":
                userService.unfollow(user_id, args.unfollow)
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
                movieService.watch_movie(args.movie_id, user_id)
            elif args.command == "watch_collection":
                collectionService.watch_collection(args.collection_id, user_id)
            elif args.command == "rate_movie":
                movieService.rate_movie(user_id, args.movie_id, args.rating)
            elif args.command == "create_collection":
                collectionService.create_collection(args.collectionid, user_id.userid, args.collection_name)
            elif args.command == "add_to_collection":
                collectionService.add_to_collection(user_id, args.collectionid, args.movie_name)
            elif args.command == "remove_from_collection":
                collectionService.remove_from_collection(args.collection_id, args.movie_id)
            elif args.command == "list_collections":
                collectionService.list_collections(user_id)
            elif args.command == "rename_collection":
                collectionService.rename_collection(args.old_name, args.new_name)
            elif args.command == "delete_collection":
                collectionService.delete_collection(args.collection_name)
            elif args.command == "list_following":
                userService.list_following()
            elif args.command == "list_followers":
                userService.list_followers()
            elif args.command == "list_users":
                userService.list_users()
            elif args.command == "list_movies":
                movieService.list_movies()
            elif args.command == "popular_movies":
                if args.followed and user_id:
                    print("Showing popular movies among users you follow:")
                    movieService.get_popular_movies_from_followed_users(user_id)
                else:
                    print("Showing popular movies from the last 90 days:")
                    movieService.get_popular_movies_last_90_days()
            elif args.command == "new_releases":
                print("Showing top new releases for this month:")
                movieService.get_top_new_releases_of_month()
            elif args.command == "exit":
                print("Exiting...")
                break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()

