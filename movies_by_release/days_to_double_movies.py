import logging, sys, math
from datetime import date, timedelta
from tmdbv3api import TMDb, Discover

logger = logging.getLogger(__name__)

tmdb = TMDb()
tmdb.wait_on_rate_limit = True

discover_endpoint = Discover()

def days_until_num_movies(start_date, num_movies):
    end_date = date.today()
    params = {
        'region': "US",
        'runtime.gte': 40,
        'release_date.gte': start_date,
        'release_date.lte': end_date
    }
    data = discover_endpoint.discover_movies(params)
    
    if (data.total_results < num_movies):
        # Then there hasn't been enough time to get to num_movies
        logger.warning(f"Only {data.total_results} movies since {start_date} is less than {num_movies}")
        return None

    timediff = end_date - start_date
    while (data.total_results != num_movies and timediff >= timedelta(days=1)):
        timediff = timedelta(days=math.floor(timediff.days / 2))
        
        if data.total_results > num_movies:
            # Then we have too many results. Cut end_date in half.
            end_date = end_date - timediff
        elif data.total_results < num_movies:
            # Then we cut too far. Go back by half.
            timediff = timediff + timedelta(days=1) # Add a day in case we overshot.
            end_date = end_date + timediff

        params['release_date.lte'] = end_date
        data = discover_endpoint.discover_movies(params)
        logger.debug(f"data.total_results={data.total_results}, timediff={timediff.days} days, end_date={end_date}")

    return end_date - start_date

def count_movies_before(start_date):
    daybefore = start_date - timedelta(days=1)
    params = {
        'region': "US",
        'runtime.gte': 40,
        'release_date.lte': daybefore.isoformat()}
    data = discover_endpoint.discover_movies(params)

    return data.total_results

def main():
    # Configure logging.
    logger.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)

    the_date = date.fromisoformat('2009-06-18')
    num_movies = count_movies_before(the_date)
    logger.info(f"num movies before {the_date}={num_movies}")
    days = days_until_num_movies(the_date, num_movies)
    if days:
        logger.info(f"days until double movies={days.days} days (={round(days.days / 365.25, 2)} years), date={the_date+days}")

if __name__ == "__main__":
    main()
