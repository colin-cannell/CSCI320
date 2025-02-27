from datetime import datetime


class Person:
    def __init__(self, first, last):
        self.first = first
        self.last = last
        self.name = first + " " + last

    def __str__(self):
        return self.name

class Movie:
    def __init__(self, genre, length, title, mpaa_rating, release_date, release_platform, movie_id, studio, director, cast):
        self.title = title
        self.genre = genre
        self.release_platform = release_platform
        self.release_date = release_date
        self.length = length
        self.mpaa_rating = mpaa_rating
        self.cast = cast
        self.director = director
        self.studio = studio

    def __str__(self):
        return f"{self.title} ({self.release_date})"

class Collection:
    def __init__(self, name):
        self.name = name
        self.movies = []

    def add_movie(self, movie):
        self.movies.append(movie)

    def remove_movie(self, movie):
        self.movies.remove(movie)

    def rename(self, new_name):
        self.name = new_name

    def get_total_runtime(self):
        total = 0
        for movie in self.movies:
            total += movie.length
        return total

    def __str__(self):
        return self.name + " (" + str(len(self.movies)) + " " + str(self.get_total_runtime())+ ")"

class User:
    def __init__(self, username, password, first_name, last_name, email, creation_date):
        self.username = username
        self.password = password
        self.email = email
        self.creation_date = creation_date
        self.last_login_date = None
        self.followers = []
        self.following = []
        self.collections = []
        self.watched = []
        self.login = False

    def follow(self, user):
        self.following.append(user)
        user.followers.append(self)

    def unfollow(self, user):
        self.following.remove(user)
        user.followers.remove(self)

    def watch(self, movie):
        self.watched.append((movie.title, datetime.now()))
        # split up date and time to store separately

    def rate(self, movie, rating):
        if 1 <= rating <= 5:
            movie.ratings.append(rating)
        else:
            print("Rating must be between 1 and 5")

    def create_collection(self, name):
        if name not in self.collections:
            collection = Collection(name)
            self.collections.append(collection)

    def add_to_collection(self, name, movie):
        collection = next((col for col in self.collections if col.name == name), None)
        if not collection:
            collection = Collection(collection)
            self.collections.append(collection)
        collection.add_movie(movie)

    def remove_from_collection(self, name, movie):
        collection = next((col for col in self.collections if col.name == name), None)
        if not collection:
            collection = Collection(len(self.collections) + 1, name)
            self.collections.append(collection)
        collection.remove_movie(movie)

    def login(self, password):
        if self.password == password:
            self.login = True
            self.last_login_date = datetime.now()

