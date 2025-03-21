import psycopg2
from psycopg2 import sql

class SocialService:
    def __init__(self, db_params):
        self.db_params = db_params

    def connect_db(self):
        try:
            return psycopg2.connect(**self.db_params)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None

    def follow(self, user_email, target_email):
        connection = self.connect_db()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            user_id = self.get_user_id(cursor, user_email)
            target_id = self.get_user_id(cursor, target_email)

            if user_id and target_id:
                cursor.execute("INSERT INTO following (userid, followingid) VALUES (%s, %s)", (user_id, target_id))
                cursor.execute("INSERT INTO followed (userid, followedid) VALUES (%s, %s)", (target_id, user_id))
                connection.commit()
                print(f"{user_email} is now following {target_email}")
                return True
            else:
                print("One or both users not found.")
                return False

        except psycopg2.Error as e:
            print(f"Error following user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def  unfollow(self, user_email, target_email):
        connection = self.connect_db()
        if not connection:
            return False

        try: 
            cursor = connection.cursor()
            user_id = self.get_user_id(cursor, user_email)
            target_id = self.get_user_id(cursor, target_email)

            if user_id and target_id:
                cursor.execute("DELETE FROM following WHERE userid = %s AND followingid = %s", (user_id, target_id))
                cursor.execute("DELETE FROM followed WHERE userid = %s AND followedid = %s", (target_id, user_id))
                connection.commit()
                print(f"{user_email} has unfollowed {target_email}")
                return True
            else:
                print("One or both users not found")
                return False

        except psycopg2.Error as e:
            print(f"Error unfollowing user: {e}")
            return False
        finally:
            cursor.close()
            connection.close()
            