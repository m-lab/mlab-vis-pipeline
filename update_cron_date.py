import argparse
import datetime
import requests
import sys


def _find_last_date_from_response(response):
    """Parses the API response to find the last date with data.

    Args:
        The JSON response from the data API
    Returns:
        Date in the form YYYY-MM-DD
    """
    # Array, expected in time order.
    results = response['results']

    # Whole range has no data
    if len(results[0]) == 1:
        return False

    # If some of the range has data, want to get the first day with nothing.
    for day in results:
        try:
            day['count']
            continue
        except KeyError:
            return day['date']



def _make_request(start_date, end_date, project_id):
    """Makes get request from data API and returns the response.

    Args:
        start_date: beginning date of time range for data
        end_date: last date of time range for data
        project_id: the Google Cloud project id
    Returns:
        Response from the API
    """
    api_url='https://data-api-dot-{gae_project}.appspot.com'\
        '/locations/naus/metrics?startdate={startdate}&enddate={enddate}'.format(
            gae_project=project_id,startdate=start_date, enddate=end_date)

    response = requests.get(api_url).json()
    return response

def find_time_range(project_id, delta=10):
    """Finds the time range to query the API with. End date is always the
    current day and start date is the last day that returns no data.

    Args:
        delta: Size in days of the time delta from current date. Default is 10.
    Returns:
        A tuple of the start and end date. Date format is YYYY-MM-DD
    """
    end_date=datetime.date.today()
    start_date=end_date - datetime.timedelta(days=delta)

    # Base case, went all the way back to the beginning of M-Lab
    if start_date <= datetime.date(year=2009, month=1, day=1):
        return str(datetime.date(year=2009, month=1, day=1)), str(end_date)

    response = _make_request(start_date, end_date, project_id)
    last_date = _find_last_date_from_response(response)

    if not last_date:
        return find_time_range(project_id, delta=delta+10)

    return str(start_date), str(end_date)

def parse_command_line(cli_args=None):
    """Parses command-line arguments.

    Args:
      cli_args: Optional array of strings to parse. Uses sys.argv by default.
    Returns:
      Google Cloud project id
    """
    if cli_args is None:
        cli_args = sys.argv[1:]

    # Parse the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='mlab-oti', nargs=1)
    args = parser.parse_args(cli_args)

    return args.project[0]

def main():
    project_id = parse_command_line()
    print find_time_range(project_id)


if __name__ == '__main__':
    main()
