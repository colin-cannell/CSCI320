import csv
import json
from datetime import datetime
from entities.movie import Movie

import json

def safe_parse_list(data):
    """Ensure the input is a list. Convert comma-separated strings into lists if necessary."""
    if isinstance(data, str):
        data = data.strip()
        if data.startswith("[") and data.endswith("]"):  # Proper JSON list format
            try:
                return json.loads(data.replace("'", "\""))  # Convert single to double quotes if needed
            except json.JSONDecodeError:
                # Fallback to manually splitting the string by comma if JSON format is invalid
                return data.split(", ")
        return data.split(", ") if ", " in data else [data]  # Convert to a list if it's a simple string
    return [data]  # If not a string, just return the data in a list

def load_movies_from_csv(file_path):
    movies = []

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row.get('Title', 'Unknown')

            # Ensure genre is a string, not a list
            genre = row.get('Genre', 'Unknown').strip()

            release_platform = row.get('Release Platform', 'Unknown').strip()

            release_date_str = row.get('Release Date', '').strip()
            try:
                release_date = datetime.strptime(release_date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                release_date = 'Unknown'

            length = row.get('Length (minutes)', '0').strip()
            length = int(length) if length.isdigit() else 0

            mpaa_rating = row.get('MPAA Rating', 'NR')

            # Ensure cast and directors are lists
            cast = safe_parse_list(row.get('Cast Members', 'Unknown'))
            directors = safe_parse_list(row.get('Directors', 'Unknown'))
            studio = row.get('Studios', 'Unknown').strip()  # Ensure studio is a string, not a list

            # Create the movie object
            movie = Movie(
                title=title, 
                genre=genre, 
                release_platform=release_platform, 
                release_date=release_date, 
                length=length, 
                cast=cast, 
                directors=directors, 
                studio=studio, 
                mpaa_rating=mpaa_rating
            )

            movies.append(movie)

    return movies