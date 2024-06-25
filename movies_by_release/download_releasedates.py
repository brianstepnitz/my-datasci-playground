"""
Uses the "The Movie Database (TMDb)" API to find all movies released in
the U.S. and writes their release date and name to files.
"""
import sys
import math
import time
import logging
import csv
import os
from tmdbv3api import TMDb, Discover, Movie
from datetime import date, timedelta

logger = logging.getLogger(__name__)

"""
Configure the TMDb API
"""
tmdb = TMDb()
# Assumes an environment variable
# "TMDB_API_KEY" set to your TMDb API key.

tmdb.wait_on_rate_limit = True
tmdb.cache = False

discover_endpoint = Discover()
movie_endpoint = Movie()

def discover_movies_between(
        start_date,
        end_date,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        page=1,
        retries=math.inf
        ):
    """
    Discover movies released between the specified start and end dates.

    Parameters:
    start_date (date): The start date for movie discovery.
    end_date (date): The end date for movie discovery.
    min_runtime_mins (int, optional): Minimum runtime of movies in
        minutes. Default is None.
    one_of_genre_ids (list of int, optional): List of genre IDs to
        filter movies by. Default is None.
    page (int, optional): Page number for pagination. Default is 1.
    retries (int, optional): Number of retry attempts in case of API
        request failure. Default is math.inf.

    Returns:
    dict: A dictionary containing movie discovery results.

    Raises:
    RuntimeError: If the movie discovery fails after the specified
        number of retries.
    """
    params = {
        'region': 'US',
        'primary_release_date.gte': start_date.isoformat(),
        'primary_release_date.lte': end_date.isoformat(),
        'sort_by': 'primary_release_date.asc',
        'page': page
    }

    if min_runtime_mins is not None:
        params['runtime.gte'] = min_runtime_mins

    if one_of_genre_ids is not None:
        params['with_genres'] = "|".join(one_of_genre_ids)

    data = None
    attempts = 0
    while (data is None and attempts < retries):
        if retries < math.inf:
            attempts += 1
        try:
            data = discover_endpoint.discover_movies(params)
        except Exception as e:
            logger.error("Exception in discover_movies_between("
                         f"start_date={start_date}, "
                         f"end_date={end_date}, "
                         f"min_runtime_mins={min_runtime_mins}, "
                         f"one_of_genre_ids={one_of_genre_ids}, "
                         f"page={page}, "
                         f"retries={retries}"
                         f") on attempts={attempts}",
                         exc_info=e)
            if attempts < retries:
                logger.error("Clearing cache and trying again.")
                discover_endpoint.cache_clear()
                data = None
                time.sleep(1)

    if data is None:
        raise RuntimeError("Could not discover movies with "
                           f"start_date>={start_date}, "
                           f"end_date<={end_date}, "
                           f"min_runtime_mins={min_runtime_mins}, "
                           f"one_of_genre_ids={one_of_genre_ids}, "
                           f"page={page}")

    return data

def discover_lte_500pages_movies_between(
        start_date,
        end_date,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        retries=math.inf):
    """
    Discover up to 500 pages worth of movies released between the
    specified start and end dates. If there is more than 500 pages of
    movies, it recursively halves the date range starting from the same
    start date until there are less than 500 pages.

    Parameters:
    start_date (date): The start date for movie discovery.
    end_date (date): The end date for movie discovery.
    min_runtime_mins (int, optional): Minimum runtime of movies in
        minutes. Default is None.
    one_of_genre_ids (list of int, optional): List of genre IDs to
        filter movies by. Default is None.
    page (int, optional): Page number for pagination. Default is 1.
    retries (int, optional): Number of retry attempts in case of API
        request failure. Default is math.inf.

    Returns:
    dict: A dictionary containing movie discovery results.
    date: The end date of the data.

    Raises:
    RuntimeError: If the movie discovery fails after the specified
        number of retries.
    """
    data = discover_movies_between(
        start_date=start_date,
        end_date=end_date,
        min_runtime_mins=min_runtime_mins,
        one_of_genre_ids=one_of_genre_ids,
        page=1,
        retries=retries)

    while data.total_pages > 500:
        timediff = end_date - start_date
        end_date = start_date + (timediff / 2)

        data = discover_movies_between(
            start_date=start_date,
            end_date=end_date,
            min_runtime_mins=min_runtime_mins,
            one_of_genre_ids=one_of_genre_ids,
            page=1,
            retries=retries)

    return data, end_date

def confirm_details(
        movie_id,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        retries=math.inf):
    """
    Confirm that the details for the movie given by the specified TMDb
    movie ID matches the specified minimum runtime minutes and one of
    the specified genre IDs. If either is None, don't confirm that
    detail. If both are None, simply return True.

    Parameters:
    movie_id (int): The TMDb ID of the movie to confirm.
    min_runtime_mins (int, optional): Minimum runtime of the movie in
        minutes. Default is None (don't confirm minimum runtime).
    one_of_genre_ids (list of int, optional): List of genre IDs to
        match with the movie. Default is None (don't confirm matching
        genre).
    retries (int, optional): Number of retry attempts in case of API
        request failure. Default is math.inf. Return False if details
        cannot be confirmed.

    Returns:
    boolean: True if the details can be confirmed, False otherwise.
    """
    if (min_runtime_mins is not None) or (one_of_genre_ids is not None):
        details = None
        attempts = 0
        while (details is None and attempts < retries):
            if retries < math.inf:
                attempts += 1
            try:
                details = movie_endpoint.details(movie_id)
            except Exception as e:
                logger.error("Exception in confirm_details("
                             f"movie_id={movie_id}, "
                             f"min_runtime_mins={min_runtime_mins}, "
                             f"one_of_genre_ids={one_of_genre_ids}, "
                             f"retries={retries}"
                             f") on attempts={attempts}.",
                             exc_info=e)
                if attempts < retries:
                    logger.error("Clearing cache and trying again.")
                    movie_endpoint.cache_clear()
                    details = None
                    time.sleep(1)

        if details is None:
            logger.warn("Could not confirm details for "
                        f"movie_id={movie_id}. Returning False.")
            return False

        if min_runtime_mins is not None and details.runtime < min_runtime_mins:
            return False
        if one_of_genre_ids is not None:
            for item in details.genres:
                if item.id in one_of_genre_ids:
                    return True
            return False

    return True

def write_page(
        dictwriter,
        data,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        retries=math.inf):
    """
    Write the given data page to the specified file. Optionally confirm
    the given details for each movie before writing.

    Parameters:
    dictwriter (csv DictWriter): The CSV DictWriter to which to write
        the data.
    data (dict): The dictionary of TMDb data to write.
    min_runtime_mins (int, optional): Minimum runtime of a movie in
        minutes. Default is None (don't confirm minimum runtime).
    one_of_genre_ids (list of int, optional): List of genre IDs to
        match with each movie. Default is None (don't confirm matching
        genre).
    retries (int, optional): Number of retry attempts in case of API
        request failure. Default is math.inf.
    """
    for result in data.results:
        if confirm_details(
            movie_id=result.id,
            min_runtime_mins=min_runtime_mins,
            one_of_genre_ids=one_of_genre_ids,
            retries=retries):

            dictwriter.writerow(result)

def write_all_pages(
        dictwriter,
        discover_data,
        start_date,
        end_date,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        retries=math.inf):
    """
    Write all pages of data to the given file for movies between the
    specified start and end dates, starting from the given first page.
    Optionally confirm the given details for each movie before writing.

    Parameters:
    dictwriter (csv DictWriter): The CSV DictWriter to which to write
        the data.
    discover_data (dict): The first page of TMDb data to write.
    start_date (date): The start date of the movie data.
    end_date (date): The end date of the movie data.
    min_runtime_mins (int, optional): Minimum runtime of a movie in
        minutes. Default is None (don't confirm minimum runtime).
    one_of_genre_ids (list of int, optional): List of genre IDs to
        match with each movie. Default is None (don't confirm matching
        genre).
    retries (int, optional): Number of retry attempts in case of API
        request failure. Default is math.inf.
    """
    while discover_data.page <= discover_data.total_pages:
        logger.info("Writing page "
                    f"{discover_data.page} / {discover_data.total_pages} "
                    f"of movies released between {start_date} and {end_date}")
        write_page(
            dictwriter=dictwriter,
            data=discover_data,
            min_runtime_mins=min_runtime_mins,
            one_of_genre_ids=one_of_genre_ids,
            retries=retries)

        if discover_data.page < discover_data.total_pages:
            discover_data = discover_movies_between(
                start_date=start_date,
                end_date=end_date,
                min_runtime_mins=min_runtime_mins,
                one_of_genre_ids=one_of_genre_ids,
                page=discover_data.page + 1,
                retries=retries)
        else:
            break

def download_all_movie_releasedates_between(
        outdir_path,
        start_date,
        end_date,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        retries=math.inf):
    
    slice_start_date = start_date
    while (slice_start_date < end_date):
        discover_data, slice_end_date = discover_lte_500pages_movies_between(
            start_date=slice_start_date,
            end_date=end_date,
            min_runtime_mins=min_runtime_mins,
            one_of_genre_ids=one_of_genre_ids,
            retries=retries)
        
        if (not os.path.exists(outdir_path)):
            os.mkdir(outdir_path)
        
        outfile_name = \
            f"movies_from_{slice_start_date}_to_{slice_end_date}.csv"
        outfile_path = f"{outdir_path}/{outfile_name}"
        with open(
            outfile_path,
            mode='w',
            encoding='utf-8',
            newline='') as out_file:

            dictwriter = csv.DictWriter(
                out_file,
                fieldnames=['release_date', 'title'],
                extrasaction='ignore')
            dictwriter.writeheader()
            write_all_pages(
                dictwriter=dictwriter,
                discover_data=discover_data,
                start_date=slice_start_date,
                end_date=slice_end_date,
                min_runtime_mins=min_runtime_mins,
                one_of_genre_ids=one_of_genre_ids,
                retries=retries)
        
        slice_start_date = slice_end_date + timedelta(days=1)

def main():
    """
    Main function.
    """
    # Configure logging.
    logger.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)

    logdir_name = "logs"
    if (not os.path.exists(logdir_name)):
        os.mkdir(logdir_name)
    
    file_handler = logging.FileHandler(
        f"{logdir_name}/fetch_movies.log",
        mode='w')
    file_handler.setLevel(logging.WARNING)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stdout_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    logger.info("Starting!")

    # These are all of the genre IDs defined by TMDb.
    genre_ids = [
        '28', # Action
        '12', # Adventure
        '16', # Animation
        '35', # Comedy
        '80', # Crime
        '99', # Documentary
        '18', # Drama
        '10751', # Family
        '14', # Fantasy
        '36', # History
        '27', # Horror
        '10402', # Music
        '9648', # Mystery
        '10749', # Romance
        '878', # Science Fiction
        '10770', # TV Movie
        '53', # Thriller
        '10752', # War
        '37' # Western
    ]
    
    # Configure movies to fetch.
    # The earliest possible date is 1874-12-09.
    download_all_movie_releasedates_between(
        outdir_path="movies_by_release/data",
        start_date=date.fromisoformat("1874-12-09"),
        end_date=date.today(),
        min_runtime_mins=40,
        one_of_genre_ids=genre_ids,
        retries=3)

if __name__ == "__main__":
    main()
