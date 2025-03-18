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
import sys
import psycopg2
# from user_service import UserService
# from movie_service import MovieService
# from collection_service import CollectionService

# Import modules for handling business logic

db_params = {
    "host": "127.0.0.1",
    "database": "p32001_21",
    "user": "cjc1985",
    "password": "Calamity2023!",
    "port": "5432"
}

def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
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
            print("Query executed successfully.")
    
    except psycopg2.Error as e:
        print(f"Query error: {e}")
    
    finally:
        cursor.close()
        connection.close()

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
    watch_parser.add_argument("movie_id", help="ID of movie to watch")
    
    # Rate movie command
    rate_parser = subparsers.add_parser("rate_movie", help="Rate a movie")
    rate_parser.add_argument("movie_id", help="ID of movie to rate")
    rate_parser.add_argument("rating", type=float, help="Rating (0-5)")
    
    # Collection commands
    create_col_parser = subparsers.add_parser("create_collection", help="Create a new collection")
    create_col_parser.add_argument("collection_name", help="Name of the collection")
    
    add_col_parser = subparsers.add_parser("add_to_collection", help="Add movie to collection")
    add_col_parser.add_argument("collection_name", help="Name of the collection")
    add_col_parser.add_argument("movie_id", help="ID of movie to add")
    
    remove_col_parser = subparsers.add_parser("remove_from_collection", help="Remove movie from collection")
    remove_col_parser.add_argument("collection_name", help="Name of the collection")
    remove_col_parser.add_argument("movie_id", help="ID of movie to remove")
    
    subparsers.add_parser("list_collections", help="List all collections")
    
    rename_col_parser = subparsers.add_parser("rename_collection", help="Rename a collection")
    rename_col_parser.add_argument("old_name", help="Current collection name")
    rename_col_parser.add_argument("new_name", help="New collection name")
    
    delete_col_parser = subparsers.add_parser("delete_collection", help="Delete a collection")
    delete_col_parser.add_argument("collection_name", help="Name of collection to delete")
    
    # Social commands
    subparsers.add_parser("list_following", help="List users you follow")
    subparsers.add_parser("list_followers", help="List your followers")
    subparsers.add_parser("list_users", help="List all users")
    
    # Movie listing command
    subparsers.add_parser("list_movies", help="List all movies")
    
    args = parser.parse_args()
    
    # Initialize service objects
    # user_service = UserService()
    # movie_service = MovieService()
    # collection_service = CollectionService()
    # social_service = SocialService()
    
    # Handle commands
    if args.command == "register":
        # user_service.register(args.username, args.password, args.first_name, args.last_name, args.email)
        print("register not implemented")
    elif args.command == "login":
        # user_service.login(args.username, args.password)
        print("login not implemented")
    elif args.command == "follow":
        # social_service.follow(args.email)
        print("follow not implemented")
    elif args.command == "unfollow":
        # social_service.unfollow(args.email)
        print("unfollow not implemented")
    elif args.command == "search_movies":
        if args.title:
            # movie_service.search_by_title(args.title)
            print("movie search by title not implemented")
        elif args.release_date:
            # movie_service.search_by_release_date(args.release_date)
            print("release data not implemented")
        elif args.cast:
            # movie_service.search_by_cast(args.cast)
            print("search by cast not implemented")
        elif args.studio:
            # movie_service.search_by_studio(args.studio)
            print("search by studio not implemented")
        elif args.genre:
            # movie_service.search_by_genre(args.genre)
            print("search by genre not setup")
    elif args.command == "sort_movies":
        # movie_service.sort_movies(args.by, args.order)
        print("sort movies not implemented")
    elif args.command == "watch_movie":
        # movie_service.watch_movie(args.movie_id)
        print("watch movie not implemented")
    elif args.command == "rate_movie":
        # movie_service.rate_movie(args.movie_id, args.rating)
        print("rate movie not implemented")
    elif args.command == "create_collection":
        # collection_service.create_collection(args.collection_name)
        print("create collection not implemented")
    elif args.command == "add_to_collection":
        # collection_service.add_to_collection(args.collection_name, args.movie_id)
        print("add to collection not implemented")
    elif args.command == "remove_from_collection":
        # collection_service.remove_from_collection(args.collection_name, args.movie_id)
        print("remove from collection not implemented")
    elif args.command == "list_collections":
        # collection_service.list_collections()
        print("list collections not implemented")
    elif args.command == "rename_collection":
        # collection_service.rename_collection(args.old_name, args.new_name)
        print("rename collection not implemented")
    elif args.command == "delete_collection":
        # collection_service.delete_collection(args.collection_name)
        print("delete collection not implemented")
    elif args.command == "list_following":
        # social_service.list_following()
        print("list following not implemented")
    elif args.command == "list_followers":
        # social_service.list_followers()
        print("list followers not implemented")
    elif args.command == "list_users":
        # user_service.list_users()
        print("list users not implemented")
    elif args.command == "list_movies":
        # movie_service.list_movies()
        print("list movies not implemented")
    else:
        parser.print_help()


    # parser = argparse.ArgumentParser(description="PostgreSQL CLI Tool")
    # parser.add_argument("query", help="SQL query to execute")
    # args = parser.parse_args()
    
    # execute_query(args.query)

        
if __name__ == "__main__":
    main()