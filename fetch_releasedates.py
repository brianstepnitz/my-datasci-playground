import logging
from tmdbv3api import Discover
import math
from datetime import timedelta
import time
import asyncio
import concurrent.futures

logger = logging.getLogger(__name__)

def main():
    # Configure Discover endpoint for TMDb API
    discover_endpoint = Discover()
    discover_endpoint.wait_on_rate_limit = True
    discover_endpoint.cache = False

async def download_all_movie_releasedates_between(
        discover_endpoint,
        start_date,
        end_date,
        min_runtime_mins=None,
        one_of_genre_ids=None,
        retries=math.inf):
    
    slice_start_date = start_date
    while (slice_start_date < end_date):
        discover_data, slice_end_date = discover_lte500pages_movies_between(
            start_date=slice_start_date,
            end_date=end_date,
            min_runtime_mins=min_runtime_mins,
            one_of_genre_ids=one_of_genre_ids,
            retries=retries)
        
        results = [discover_data]
        
        if (discover_data.total_pages > 1):
            results.extend(
                await fetch_all_pages(
                    discover_endpoint=discover_endpoint,
                    start_date=slice_start_date,
                    end_date=slice_end_date,
                    total_pages=discover_data.total_pages,
                    min_runtime_mins=min_runtime_mins,
                    one_of_genre_ids=one_of_genre_ids,
                    retries=retries
                )
            )
        
        # TODO Write the results to a file.
        
        slice_start_date = slice_end_date + timedelta(days=1)

def discover_lte500pages_movies_between(
        discover_endpoint,
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
    discover_endpoint (TMDb): The TMDb Discover endpoint.
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
        discover_endpoint=discover_endpoint,
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

def discover_movies_between(
        discover_endpoint,
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
    discover_endpoint (TMDb): The TMDb Discover endpoint.
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

async def fetch_all_pages(
        discover_endpoint,
        start_date,
        end_date,
        total_pages,
        min_runtime_mins,
        one_of_genre_ids,
        retries):
    
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                discover_movies_between,
                discover_endpoint,
                start_date,
                end_date,
                min_runtime_mins,
                one_of_genre_ids,
                page,
                retries)
            for page in range(2, total_pages)
        ]

        results = await asyncio.gather(*tasks)
        return results

if __name__ == "__main__":
    main()