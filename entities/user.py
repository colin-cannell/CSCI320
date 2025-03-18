import datetime
from entities.collection import Collection

class User:
    def __init__(self, username, password, first_name, last_name, email, creation_date):
        self.username = username
        self.first_name = first_name
        self.last_name = last_name
        self.password = password
        self.email = email
        self.creation_date = creation_date
        self.last_login_date = None
        self.followers = []
        self.following = []
        self.collections = []
        self.watched = []
        self.logged_in = False

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

    def __str__(self):
        return f"{self.username} ({self.first_name} {self.last_name})"
    
    def _get_password(self):
        return self.password
    
    def get_username(self):
        return self.username
    
    def get_email(self):
        return self.email
    
    def get_creation_date(self):
        return self.creation_date
    
    def get_last_login_date(self):
        return self.last_login_date
    
    def get_followers(self):
        return self.followers
    
    def get_following(self):
        return self.following
    
    def get_collections(self):
        return self.collections
    
    def get_watched(self):
        return self.watched
    
    def is_logged_in(self):
        return self.logged_in
    


    