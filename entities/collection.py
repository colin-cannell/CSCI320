class Collection:
    def __init__(self, name):
        self.name = name
        self.movies = []
        self.runtime = self.get_total_runtime()

    def add_movie(self, movie):
        self.movies.append(movie)

    def remove_movie(self, movie):
        self.movies.remove(movie)

    def rename(self, new_name):
        self.name = new_name

    def get_total_runtime(self):
        total = 0

        if len(self.movies) == 0:
            return 0
        
        for movie in self.movies:
            total += movie.length
        return total

    def __str__(self):
        return self.name + " (" + str(len(self.movies)) + " " + str(self.get_total_runtime())+ ")"
    
    def get_movies(self):
        return self.movies
    
    def get_name(self):
        return self.name
    
    def get_runtime(self):
        return self.runtime