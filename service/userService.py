import psycopg2
from psycopg2 import sql
import bcrypt

def hash(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def check_password(password, hashed):
    return bcrypt.checkpw(password.encode('utf-8'), hashed)

class UserService:
    def __init__(self, db_params):
        self.params = db_params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def register(self, username, password, first_name, last_name, email):
        connection = self.connect_db()
        if not connection:
            return
        
        try:
            cursor = connection.cursor()
            hashed_password = hash(password)
            query = sql.SQL("""
                            INSERT INTO users (username, password, first_name, last_name, email)
                             VALUES (%s, %s, %s, %s, %s)
                            """)
            cursor.execute(query, (username, hashed_password, first_name, last_name, email))
            connection.commit()
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
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT password FROM users WHERE username = %s
                            """)
            cursor.execute(query, (username,))
            result = cursor.fetchone()
            if result and check_password(password, result[0]):
                return True
            else:
                return False
        except psycopg2.Error as e:
            print(f"Error logging in: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def follow(self, follower_email, followee_email):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO follows (follower_email, followee_email
                            VALUES (%s, %s)
                            """)
            cursor.execute(query, (follower_email, followee_email))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error following user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def unfollow(self, follower_email, followee_email):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            DELETE FROM follows WHERE follower_email = %s AND followee_email = %s
                            """)
            cursor.execute(query, (follower_email, followee_email))
            connection.commit()
            return True
        except psycopg2.Error as e:
            print(f"Error unfollowing user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()


        