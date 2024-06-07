"""
Uses the "The Movie Database (TMDb)" API to find all movies released in the U.S. and writes their release date and name to files.
"""
import sys
import math
import time
import logging
import csv
from tmdbv3api import TMDb, Discover, Movie
from datetime import date, timedelta

logger = logging.getLogger(__name__)

"""
Configure the TMDb API
"""
tmdb = TMDb()
# Requires an environment variable "TMDB_API_KEY" set to your TMDb API key

tmdb.wait_on_rate_limit = True
# tmdb.cache = False

discover_endpoint = Discover()
movie_endpoint = Movie()

def discover_movies_between(start_date, end_date, min_runtime_mins=None, one_of_genre_ids=None, page=1, retries=math.inf):
    """
    Discover movies released between the specified start and end dates.

    Parameters:
    start_date (date): The start date for movie discovery.
    end_date (date): The end date for movie discovery.
    min_runtime_mins (int, optional): Minimum runtime of movies in minutes. Default is None.
    one_of_genre_ids (list of int, optional): List of genre IDs to filter movies by. Default is None.
    page (int, optional): Page number for pagination. Default is 1.
    retries (int, optional): Number of retry attempts in case of API request failure. Default is math.inf.

    Returns:
    dict: A dictionary containing movie discovery results.

    Raises:
    RuntimeError: If the movie discovery fails after the specified number of retries.

    Example:
    >>> from datetime import date
    >>> discover_movies_between(date(2020, 1, 1), date(2020, 12, 31), min_runtime_mins=90, one_of_genre_ids=[28, 12])
    {'page': 1, 'results': [...], 'total_pages': 10, 'total_results': 200}
    """
    params = {
        'region': 'US',
        'primary_release_date.gte': start_date.isoformat(),
        'primary_release_date.lte': end_date.isoformat(),
        'sort_by': 'primary_release_date.asc',
        'page': page
    }

    if min_runtime_mins:
        params['runtime.gte'] = min_runtime_mins

    if one_of_genre_ids:
        params['with_genres']: "|".join(one_of_genre_ids)

    data = None
    tries = 0
    while (not data and tries < retries):
        if retries < math.inf:
            tries = tries + 1
        try:
            data = discover_endpoint.discover_movies(params)
        except Exception as e:
            logger.error(f"Exception in discover_movies_between(start_date={start_date}, end_date={end_date}, min_runtime_mins={min_runtime_mins}, one_of_genre_ids={one_of_genre_ids}, page={page}, retries={retries}) on tries={tries}", exc_info=e)
            if tries < retries:
                logger.error("Clearing cache and trying again.")
                discover_endpoint.cache_clear()
                data = None
                time.sleep(1)

    if not data:
        raise RuntimeError(f"Could not discover movies with start_date>={start_date}, end_date<={end_date}, min_runtime_mins={min_runtime_mins}, one_of_genre_ids={one_of_genre_ids}, page={page}")

    return data

def discover_lte_500pages_movies_between(start_date, end_date, min_runtime_mins=None, one_of_genre_ids=None, retries=math.inf):
    """
    Discover up to 500 pages worth of movies released between the specified start and end dates. If there is more than 500 pages of movies,
    it recursively halves the date range starting from the same start date until there are less than 500 pages.

    Parameters:
    start_date (date): The start date for movie discovery.
    end_date (date): The end date for movie discovery.
    min_runtime_mins (int, optional): Minimum runtime of movies in minutes. Default is None.
    one_of_genre_ids (list of int, optional): List of genre IDs to filter movies by. Default is None.
    page (int, optional): Page number for pagination. Default is 1.
    retries (int, optional): Number of retry attempts in case of API request failure. Default is math.inf.

    Returns:
    dict: A dictionary containing movie discovery results.

    Raises:
    RuntimeError: If the movie discovery fails after the specified number of retries.
    """
    data = discover_movies_between(start_date, end_date, min_runtime_mins, one_of_genre_ids, page=1, retries=retries)

    while data.total_pages > 500:
        timediff = end_date - start_date
        end_date = start_date + (timediff / 2)

        data = discover_movies_between(start_date, end_date, min_runtime_mins, one_of_genre_ids, page=1, retries=retries)

    data['end_date'] = end_date
    return data

def confirm_details(movie_id, min_runtime_mins=None, one_of_genre_ids=None, retries=math.inf):
    """
    Confirm that the details for the movie given by the specified TMDb movie ID matches the specified minimum runtime minutes and one of the specified genre IDs.
    If either is None, don't confirm that detail. If both are None, simply return True.

    Parameters:
    movie_id (int): The TMDb ID of the movie to confirm.
    min_runtime_mins (int, optional): Minimum runtime of the movie in minutes. Default is None (don't confirm minimum runtime).
    one_of_genre_ids (list of int, optional): List of genre IDs to match with the movie. Default is None (don't confirm matching genre).
    retries (int, optional): Number of retry attempts in case of API request failure. Default is math.inf. Return False if details cannot be confirmed.

    Returns:
    boolean: True if the details can be confirmed, False otherwise.
    """
    if min_runtime_mins or one_of_genre_ids:
        details = None
        tries = 0
        while (not details and tries < retries):
            if retries < math.inf:
                tries = tries + 1
            try:
                details = movie_endpoint.details(movie_id)
            except Exception as e:
                logger.error(f"Exception in confirm_details(movie_id={movie_id}, min_runtime_mins={min_runtime_mins}, one_of_genre_ids={one_of_genre_ids}, retries={retries}) on tries={tries}.", exc_info=e)
                if tries < retries:
                    logger.error("Clearing cache and trying again.")
                    movie_endpoint.cache_clear()
                    details = None
                    time.sleep(1)

        if not details:
            logger.warn(f"Could not confirm details for movie_id={movie_id}. Returning False.")
            return False

        if min_runtime_mins and details.runtime < min_runtime_mins:
            return False
        if one_of_genre_ids:
            for item in details.genres:
                if item.id in one_of_genre_ids:
                    return True
            return False

    return True

def write_page(csvwriter, data, min_runtime_mins=None, one_of_genre_ids=None, retries=math.inf):
    """
    Write the given data page to the specified file. Optionally confirm the given details for each movie before writing.

    Parameters:
    csvwriter (csv writer): The CSV Writer to which to write the data.
    data (dict): The dictionary of TMDb data to write.
    min_runtime_mins (int, optional): Minimum runtime of a movie in minutes. Default is None (don't confirm minimum runtime).
    one_of_genre_ids (list of int, optional): List of genre IDs to match with each movie. Default is None (don't confirm matching genre).
    retries (int, optional): Number of retry attempts in case of API request failure. Default is math.inf.
    """
    for result in data.results:
        if confirm_details(result.id, min_runtime_mins, one_of_genre_ids, retries):
            csvwriter.writerow([result.release_date, result.title])

def write_all_pages(csvwriter, firstpage_data, start_date, end_date, min_runtime_mins=None, one_of_genre_ids=None, retries=math.inf):
    """
    Write all pages of data to the given file for movies between the specified start and end dates, starting from the given first page.
    Optionally confirm the given details for each movie before writing.

    Parameters:
    csvwriter (csv writer): The CSV Writer to which to write the data.
    firstpage_data (dict): The first page of TMDb data to write.
    start_date (date): The start date of the movie data.
    end_date (date): The end date of the movie data.
    min_runtime_mins (int, optional): Minimum runtime of a movie in minutes. Default is None (don't confirm minimum runtime).
    one_of_genre_ids (list of int, optional): List of genre IDs to match with each movie. Default is None (don't confirm matching genre).
    retries (int, optional): Number of retry attempts in case of API request failure. Default is math.inf.
    """
    data = firstpage_data
    while data.page < data.total_pages:
        logger.info(f"start_date={start_date}, end_date={end_date}, page= {data.page} / {data.total_pages}")
        write_page(csvwriter, data, min_runtime_mins, one_of_genre_ids, retries)

        data = discover_movies_between(start_date, end_date, min_runtime_mins, one_of_genre_ids, page=(data.page + 1), retries=retries)

    # Write the last page of data.
    logger.info(f"start_date={start_date}, end_date={end_date}, page= {data.page} / {data.total_pages}")
    write_page(csvwriter, data, min_runtime_mins, one_of_genre_ids, retries)

def main():
    """
    Main function.
    """
    # Configure logging
    logger.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler("logs/fetch_movies.log", mode='w')
    file_handler.setLevel(logging.WARNING)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stdout_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    # Configure movies to fetch
    start_date = date.fromisoformat('1874-12-09') # The earliest possible date is 1874-12-09
    today = date.today()
    end_date = today

    genre_ids = [
        28, # Action
        12, # Adventure
        16, # Animation
        35, # Comedy
        80, # Crime
        99, # Documentary
        18, # Drama
        10751, # Family
        14, # Fantasy
        36, # History
        27, # Horror
        10402, # Music
        9648, # Mystery
        10749, # Romance
        878, # Science Fiction
        10770, # TV Movie
        53, # Thriller
        10752, # War
        37 # Western
    ]

    min_runtime_mins = 40
    retries = 3
    csvfiles_dirname = "data"
    while (start_date < today):
        data = discover_lte_500pages_movies_between(start_date, end_date, min_runtime_mins=min_runtime_mins, one_of_genre_ids=genre_ids, retries=retries)
        end_date = data['end_date']

        with open(f"{csvfiles_dirname}/movies_from_{start_date}_to_{end_date}.csv", 'w', encoding='utf-8', newline='') as f:
            csvwriter = csv.writer(f)
            csvwriter.writerow(["#Release Date", "#Title"])
            write_all_pages(csvwriter, data, start_date, end_date, min_runtime_mins=min_runtime_mins, one_of_genre_ids=genre_ids, retries=retries)

        start_date = end_date + timedelta(days=1)
        end_date = today

if __name__ == "__main__":
    main()
