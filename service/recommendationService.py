import psycopg2
from psycopg2 import sql

class RecommendationService:
    def __init__(self, params):
        self.params = params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
    
    def get_user_watch_history(self, userid):
        # [movie, rating]
        connection = self.connect_db()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            query = sql.SQL("""SELECT movieid, rating 
                            FROM movierating 
                            WHERE userid = %s""")
            cursor.execute(query, (userid,))
            results = cursor.fetchall()
            return results
        except psycopg2.Error as e:
            print(f"Error fetching watch history: {e}")
        finally:
            cursor.close()
            connection.close()      

    def get_following_movies(self, userid):
        # [userid, movie, rating]
        connection = self.connect_db()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            query = sql.SQL("""select movieid, rating
                            from movierating
                            where userid in (select followingid from following where userid = %s)
                            """)
            cursor.execute(query, (userid,))
            results = cursor.fetchall()
            return results
        except psycopg2.Error as e:
            print(f"Error fetching following movies: {e}")
        finally:
            cursor.close()
            connection.close()

    def get_movie_info(self, movie_ids):
        connection = self.connect_db()
        if not connection:
            return False
        try:
            cursor = connection.cursor()
            query = sql.SQL(""" WITH MovieGenres AS (
                                SELECT MG.MovieID, ARRAY_AGG(G.genreid) AS Genres
                                FROM MovieGenre MG
                                LEFT JOIN Genre G ON MG.GenreID = G.GenreID
                                GROUP BY MG.MovieID
                            ),
                            MovieStudios AS (
                                SELECT MS.MovieID, S.studioid AS Studio
                                FROM MovieStudio MS
                                LEFT JOIN Studio S ON MS.StudioID = S.StudioID
                            ),
                            MovieDirectors AS (
                                SELECT MD.MovieID, D.personid AS Director
                                FROM MovieDirector MD
                                LEFT JOIN Person D ON MD.PersonID = D.PersonID
                            ),
                            MovieActors AS (
                                SELECT MA.MovieID, ARRAY_AGG(DISTINCT A.personid) AS Actors
                                FROM MovieActor MA
                                LEFT JOIN Person A ON MA.PersonID = A.PersonID
                                GROUP BY MA.MovieID
                            ),
                            MoviePlatforms AS (
                                SELECT MP.MovieID, ARRAY_AGG(DISTINCT PL.platformid) AS Platforms
                                FROM MoviePlatform MP
                                LEFT JOIN Platform PL ON MP.PlatformID = PL.PlatformID
                                GROUP BY MP.MovieID
                            )
                            SELECT
                                M.MovieID,
                                M.Length,
                                M.MpaaRating,
                                M.ReleaseDate,
                                MG.Genres,
                                MS.Studio,
                                MD.Director,
                                MA.Actors,
                                MP.Platforms
                            FROM Movie M
                            LEFT JOIN MovieGenres MG ON M.MovieID = MG.MovieID
                            LEFT JOIN MovieStudios MS ON M.MovieID = MS.MovieID
                            LEFT JOIN MovieDirectors MD ON M.MovieID = MD.MovieID
                            LEFT JOIN MovieActors MA ON M.MovieID = MA.MovieID
                            LEFT JOIN MoviePlatforms MP ON M.MovieID = MP.MovieID
                            WHERE M.MovieID IN %s
                            GROUP BY M.MovieID, M.Length, M.MpaaRating, M.ReleaseDate, MG.Genres, MS.Studio, MD.Director, MA.Actors, MP.Platforms;
                            """)
            cursor.execute(query, (tuple(movie_ids),))  # Pass the collection as a tuple
            results = cursor.fetchall()

            # Process the results into a dictionary for easy lookup by movieid
            movie_info_dict = {result[0]: list(result[1:]) for result in results}
            return movie_info_dict
            # movie : [id, length, rating, release_date, [genre], studio, director, [cast], [platform]]
        except psycopg2.Error as e:
            print(f"Error fetching movie info for multiple IDs: {e}")
            return {}
        finally:
            cursor.close()
            connection.close()

    def recommendation(self, userid):
        userMovies = self.get_user_watch_history(userid)
        if userMovies:
            Uids, uRatings = zip(*userMovies)
        else:
            Uids = []
            uRatings = []

        print("got user movies")

        followingMoviesRaw = self.get_following_movies(userid)
        followingMovies = [
            fMovie for fMovie in followingMoviesRaw
            if fMovie[0] not in Uids and fMovie[1] > 5
        ]
        if followingMovies:
            Fids, fRatings = zip(*followingMovies)
        else:
            Fids = []
            fRatings = []

        print("got following movies")

        # refactor get info to work with a list of ids
        uMovies = {}
        if Uids:
            uMovies = self.get_movie_info(Uids)
        print("got user movie info")

        fMovies = {}
        if Fids:
            fMovies = self.get_movie_info(Fids)
        print("got following movies info")

        genre_mapping = {1: "scifi", 2: "horror", 3: "thriller", 4: "documentary", 5: "comedy", 6: "drama", 7: "romance", 8: "action"}

        user_movies_by_genre = {genre: [] for genre in genre_mapping.values()}
        print("got user movies by genre")
        following_movies_by_genre = {genre: [] for genre in genre_mapping.values()}
        print("got following movies by genre")

        for movie_id in uMovies:
            movie = uMovies[movie_id]
            genre_id = movie[3][0]

            genre = genre_mapping.get(genre_id)
            if genre:
                user_movies_by_genre[genre].append(movie)
        print("clustered user movies by genre")

        for movie_id in fMovies:
            movie = fMovies[movie_id]
            genre_id = movie[3][0]
            genre = genre_mapping.get(genre_id)
            if genre:
                following_movies_by_genre[genre].append([movie, movie_id])  
        print("clustered following movies by genre")

        similarity_scores = []
        # for each movie in a genre compare it to the movies the user has watched
        for genre in genre_mapping.values():
            for movie in following_movies_by_genre[genre]:
                f_movie = movie[0]  # Get the movie info 
                f_movie_id = movie[1]
                fscore = 0
                for u_movie in user_movies_by_genre[genre]:
                    try:
                        score = self.similarity(f_movie, u_movie)
                        # Assuming userMovies is a list of (movie_id, rating) tuples
                        user_rating = next((rating for mid, rating in userMovies if mid == u_movie[0]), 0) # Get user's rating for the watched movie
                        weighted_score = score * int(user_rating)
                        if weighted_score > fscore:
                            fscore = weighted_score
                    except Exception as e:
                        print(f"Error during similarity calculation: {e}")
                        return  # Stop here and see the error
                similarity_scores.append((f_movie_id, fscore)) # Store movie ID and weighted score
                    

        print("calculated similarity scores")
        similarity_scores.sort(key=lambda x: x[1], reverse=True)  # Sort by weighted score

        # Get top 10 recommendations
        top_recommendations = similarity_scores[:10]
        print("Top Recommedned Movies")
        for rec in top_recommendations:
            if rec[1] > 0:
                print(rec[0], fMovies[rec[0]], rec[1])

        # add in getting the title of the movie as well
        

        return True
       
# ssh -N -L 40000:127.0.0.1:5432 cjc1985@starbug.cs.rit.edu
    
    def similarity(self, movie1, movie2):
        # movie : [length, rating, release_date, [genre], studio, director, [cast], [platform]]

        wL = .1
        wG = .4
        wS = .1
        wD = .2
        wR = .1
        wC = .3

        def binary(x, y):
            if x == y:
                return 1
            return 0
        
        def ratio(x, y):
            n = x - y
            d = max(x, y)
            return 1 - (n/d)
        
        def jaccard(x, y):
            intersect = len(x.intersection(y))
            union = len(x.union(y))

            if union == 0:
                return 0
            else:
                return intersect / union
            
        length = ratio(movie1[0], movie2[0]) * wL
        rating = binary(movie1[1], movie2[1]) * wR
        genre = binary(movie1[3][0], movie2[3][0]) * wG
        studio = binary(movie1[5], movie2[5]) * wS
        director = binary(movie1[6], movie2[6]) * wD
        cast = jaccard(set(movie1[7]), set(movie2[7])) * wC
       
        return length + rating + genre + studio + director + cast
            
        

            

# at least 2 complex queries 

            
"""
ssh -N -L 40000:localhost:5432 cjc1985@starbug.cs.rit.edu
recommendation 1
"""