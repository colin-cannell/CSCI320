import csv
import json
from datetime import datetime
from main import Movie  # Ensure this import is correct


def safe_parse_list(data):
    """Ensure the input is a list. Convert comma-separated strings into lists if necessary."""
    if isinstance(data, str):
        data = data.strip()
        if data.startswith("[") and data.endswith("]"):  # Proper JSON list format
            try:
                return json.loads(data.replace("'", "\""))  # Convert single to double quotes if needed
            except json.JSONDecodeError:
                return data.split(", ")  # Fallback to splitting manually
        return data.split(", ") if ", " in data else [data]  # Convert to a list
    return []


def load_movies_from_csv(file_path):
    movies = []

    with open(file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row.get('Title', 'Unknown')
            genre = safe_parse_list(row.get('Genre', 'Unknown'))
            release_platform = row.get('Release Platform', 'Unknown')

            release_date_str = row.get('Release Date', '').strip()
            try:
                release_date = datetime.strptime(release_date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            except ValueError:
                release_date = 'Unknown'

            length = row.get('Length', '0').strip()
            length = int(length) if length.isdigit() else 0

            mpaa_rating = row.get('MPAA Rating', 'NR')

            cast = safe_parse_list(row.get('Cast Members', 'Unknown'))
            directors = safe_parse_list(row.get('Directors', 'Unknown'))
            studio = row.get('Studios', 'Unknown')

            movie = Movie(title, genre, release_platform, release_date, length, cast, directors, studio, mpaa_rating)

            movies.append(movie)

    return movies


movies = load_movies_from_csv('movies_dataset.csv')
for movie in movies:
    print(movie)
