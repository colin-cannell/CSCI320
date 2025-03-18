import psycopg2



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
    parser = argparse.ArgumentParser(description="PostgreSQL CLI Tool")
    parser.add_argument("query", help="SQL query to execute")
    args = parser.parse_args()
    
    execute_query(args.query)

if __name__ == "__main__":
    main()





