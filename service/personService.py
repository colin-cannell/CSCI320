import psycopg2
from psycopg2 import sql

class PersonService:
    def __init__(self, db_params):
        self.params = db_params

    def connect_db(self):
        try:
            conn = psycopg2.connect(**self.params)
            return conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def add_person(self, first_name, last_name):
        connection = self.connect_db()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            INSERT INTO Person ( FirstName, LastName)
                            VALUES (%s, %s)
                            """)
            cursor.execute(query, (first_name, last_name))
            connection.commit()
            print("Person added successfully.")
            return True
        except psycopg2.Error as e:
            print(f"Error adding person: {e}")
            return False
        finally:
            cursor.close()
            connection.close()

    def get_person(self, first_name, last_name):
        connection = self.connect_db()
        if not connection:
            return None

        try:
            cursor = connection.cursor()
            query = "SELECT PersonID FROM Person WHERE FirstName = %s AND LastName = %s"
            cursor.execute(query, (first_name, last_name))
            return cursor.fetchone()  # This returns a tuple, not a list
        except psycopg2.Error as e:
            print(f"Error fetching person: {e}")
            return None
        finally:
            cursor.close()
            connection.close()

    def get_people(self):
        connection = self.connect_db()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            query = sql.SQL("""
                            SELECT * FROM Person
                            """)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except psycopg2.Error as e:
            print(f"Error fetching people: {e}")
            return None
        finally:
            cursor.close()
            connection.close()
