import psycopg2
from psycopg2 import sql
import bcrypt

def hash_password(password):
    """Hashes the password using bcrypt."""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def check_password(password, hashed):
    """Checks if a password matches a hashed password."""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

class UserService:
    def __init__(self, db_params):
        self.db_params = db_params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.db_params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
    
    def get_user_id(self, username):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT UserID FROM "User" WHERE Username = %s
                        """)
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                return None
        except psycopg2.Error as e:
            print(f"Error fetching user ID: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
        
    def register(self, username, password, first_name, last_name, email):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            hashed_password = hash_password(password)
            query = """
                    INSERT INTO "User" (Username, Password, FirstName, LastName, Email, CreationDate, LastLogInDate)
                    VALUES (%s, %s, %s, %s, %s, DEFAULT, DEFAULT)
                """
            cursor.execute(query, (username, hashed_password, first_name, last_name, email))
            connection.commit()
            print("User registered successfully.")
            return True
        except psycopg2.Error as e:
            print(f"Error registering user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def login(self, username, password):
        connection = self.connect_db()
        if not connection:
            print("Connection error.")
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            UPDATE "User" 
                            SET LastLogInDate = CURRENT_TIMESTAMP 
                            WHERE Username = %s
                            """)
            cursor.execute(query, (username,))
            query = sql.SQL("""
                            SELECT Password FROM "User" WHERE Username = %s
                            """)
            cursor.execute(query, (username,))
            result = cursor.fetchone()

            if result:
                stored_hashed_password = result[0]

                if isinstance(stored_hashed_password, str) and stored_hashed_password.startswith("\\x"):
                    stored_hashed_password = bytes.fromhex(stored_hashed_password[2:])

                if bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password):
                    print("Login successful.")
                    return True
                else:
                    print("Login failed. Incorrect password.")
                    return False
            else:
                print("Login failed. User not found.")
                return False
        except psycopg2.Error as e:
            print(f"Error logging in: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def follow(self, userid, following_username):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            following_id = self.get_user_id(following_username)
            

            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO following  (userid, followingid)
                            VALUES (%s, %s)
                            """)
            cursor.execute(query, (userid, following_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error following user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def unfollow(self, userid, following_username):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            following_id = self.get_user_id(following_username)

            cursor = connection.cursor()
            query = sql.SQL("""
                            DELETE FROM following WHERE userid = %s AND followingid = %s
                            """)
            cursor.execute(query, (userid, following_id))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error unfollowing user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def get_creation_date(self, username):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT CreationDate FROM "User" WHERE Username = %s
                        """)
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            if result:
                print(f"Creation date for {username}: {result[0]}")
                return True
            else:
                print(f"User {username} not found.")
                return None
            
        except psycopg2.Error as e:
            print(f"Error fetching creation date: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
           

    def get_last_login(self, username):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT LastLogInDate FROM "User" WHERE Username = %s
                        """)
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            if result:
                last_login_date = result[0]
                formatted_last_login = last_login_date.strftime("%Y-%m-%d %H:%M:%S")
                print(f"Last login date for {username}: {formatted_last_login}")
                return True
            else:
                print(f"User {username} not found.")
                return None
            
        except psycopg2.Error as e:
            print(f"Error fetching last login date: {e}")
            return None
        finally:
            cursor.close()
            connection.close()


    def watch_movie(self, userid, movieid):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO watchhistory (movieid, userid, date)
                            VALUES (%s, %s, CURRENT_TIMESTAMP)
                            """)
            cursor.execute(query, (movieid, userid))
            connection.commit()
            print(f"User {userid} watched movie {movieid}.")
            return True
        except psycopg2.Error as e:
            print(f"Error recording watched movie: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def watch_collection(self, userid, collectionid):
       connection = self.connect_db()
       if not connection:
           return False
      
       try:
           cursor = connection.cursor()
           query = sql.SQL("""
                           INSERT INTO watchhistory (movieid, userid, date)
                           SELECT %s, movieid, CURRENT_TIMESTAMP
                           FROM collectionmovie WHERE collectionid = %s AND movieid = movieid
                           """)
           cursor.execute(query, (userid, collectionid))
           connection.commit()
           print(f"User {userid} watched collection {collectionid}.")
           return True
       except psycopg2.Error as e:
           print(f"Error recording watched collection: {e}")
           return False
       finally:
           cursor.close()
           connection.close()

    def rate_movie(self, userid, movieid, rating):
       if rating < 1 or rating > 5:
           print("Rating must be between 1 and 5.")
           return False

       connection = self.connect_db()
       if not connection:
           return False

       try:
           cursor = connection.cursor()
           query = sql.SQL("""
                           INSERT INTO movierating (movieid, userid, rating)
                           VALUES (%s, %s, %s)
                           """)
           cursor.execute(query, (movieid, userid, rating))
           connection.commit()
           print(f"User {userid} rated movie {movieid} with a {rating}.")
           return True
       except psycopg2.Error as e:
           print(f"Error rating movie: {e}")
           return False
       finally:
           cursor.close()
           connection.close()

    def get_user_profile_info(self, user_id):
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            # Number of collections
            cur.execute("SELECT COUNT(*) FROM collection WHERE userid = %s", (user_id,))
            collection_count = cur.fetchone()[0]

            # Following count
            cur.execute("SELECT COUNT(*) FROM following WHERE userid = %s", (user_id,))
            following_count = cur.fetchone()[0]

            # Follower count
            cur.execute("SELECT COUNT(*) FROM following WHERE followingid = %s", (user_id,))
            followers_count = cur.fetchone()[0]

            # Top 10 movies (by rating, then play count)
            cur.execute("""
                SELECT m.name, r.rating, COUNT(w.date) as play_count
                FROM movierating r
                JOIN movie m ON r.movieid = m.movieid
                LEFT JOIN watchhistory w ON m.movieid = w.movieid AND w.userid = r.userid
                WHERE r.userid = %s
                GROUP BY m.name, r.rating
                ORDER BY r.rating DESC, play_count DESC
                LIMIT 10
            """, (user_id,))
            top_movies = cur.fetchall()

            return {
                "collection_count": collection_count,
                "following_count": following_count,
                "followers_count": followers_count,
                "top_movies": top_movies
            }

        except Exception as e:
            print(f"Database error in get_user_profile_info: {e}")
            return None

        finally:
            try:
                cur.close()
                conn.close()
            except:
                pass
    
    def list_users(self):
        """List all users in the database with their basic information."""
        connection = self.connect_db()
        if not connection:
            return []

        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                SELECT 
                    UserID, 
                    Username, 
                    FirstName, 
                    LastName, 
                    Email, 
                    CreationDate, 
                    LastLogInDate 
                FROM "User"
                ORDER BY UserID
            """)
            
            cursor.execute(query)
            users = cursor.fetchall()
            
            if not users:
                print("No users found in the database.")
                return []
            
            print("\nList of all registered users:")
            print("{:<8} {:<15} {:<15} {:<15} {:<25} {:<15} {:<15}".format(
                "ID", "Username", "First Name", "Last Name", "Email", 
                "Created On", "Last Login"
            ))
            print("-" * 100)
            
            for user in users:
                # Format dates if they exist
                created = user[5].strftime("%Y-%m-%d") if user[5] else "N/A"
                last_login = user[6].strftime("%Y-%m-%d %H:%M") if user[6] else "Never"
                
                print("{:<8} {:<15} {:<15} {:<15} {:<25} {:<15} {:<15}".format(
                    user[0],  # UserID
                    user[1],  # Username
                    user[2],  # FirstName
                    user[3],  # LastName
                    user[4],  # Email
                    created,
                    last_login
                ))
            
            return users
            
        except psycopg2.Error as e:
            print(f"Error listing users: {e}")
            return []
        finally:
            cursor.close()
            connection.close()