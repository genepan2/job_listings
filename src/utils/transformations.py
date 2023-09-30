from datetime import datetime, timedelta
import re
from config.constants import JOB_LEVELS, JOB_LOCATIONS


def transform_job_level(job_level: str = 'Entry', job_title: str = None):
  job_level = job_level.lower().strip()
  job_title = job_title.lower().strip()

  job_levels_mapping = {
      "intern": ["internship"],
      "entry": ["entry level", "entry", "junior"],
      "mid": ["mid level", "mid"],
      "senior": ["senior level", "senior"],
      "unknown": ["not applicable"]
  }

  for level, level_categories in job_levels_mapping.items():
      if job_level in level_categories or job_title in [f"({level.lower()})", f"({level[:3].lower()})"]:
          return JOB_LEVELS[level]

  return JOB_LEVELS["unknown"]

def transform_job_location(job_location: str):
  # job_location = job_location.lower().split(',')[0].strip()
  job_location = job_location.lower()

  locations_mapping = {
      "berlin": ["berlin"],
      "munich": ["munich", "münchen"],
      "hamburg": ["hamburg"],
      "cologne": ["cologne", "köln"]
  }

  for location, location_names in locations_mapping.items():
      if any(name in job_location for name in location_names):
          return JOB_LOCATIONS.get(location)

  return JOB_LOCATIONS["unknown"]

import re
from datetime import datetime, timedelta

def transform_to_isoformat(publication_date, search_datetime):
    today_names = ["today", "heute"]
    yesterday_names = ["yesterday", "gestern"]

    # Check if publication_date is a special keyword like "today" or "yesterday"
    if publication_date and publication_date.lower() in today_names:
        return search_datetime

    if publication_date and publication_date in yesterday_names:
        # We expect search_datetime to be in ISO format
        try:
            search_datetime_ob = datetime.strptime(search_datetime, '%Y-%m-%dT%H:%M:%S.%f')
            new_date_time_obj = search_datetime_ob - timedelta(days=1)
            return new_date_time_obj.isoformat()
        except ValueError:
            pass  # Continue to handle publication_date as a date string

    # Use regular expression to extract the numeric value and unit (hour, day, week, month)
    match = re.match(r'(\d+)\s+(\w+)', publication_date)
    if match:
        value, unit = match.groups()
        value = int(value)
        unit = unit.lower()

        if unit == "second":
            return (search_datetime_ob - timedelta(seconds=value)).isoformat()
        if unit == "hour":
            return (search_datetime_ob - timedelta(hours=value)).isoformat()
        elif "day"in unit:
            return (search_datetime_ob - timedelta(days=value)).isoformat()
        elif "week"in unit:
            return (search_datetime_ob - timedelta(weeks=value)).isoformat()
        elif "month"in unit:
            return (search_datetime_ob - timedelta(month=value)).isoformat()
            # Calculate the number of days in the month
            # year = search_datetime_ob.year
            # month = search_datetime_ob.month - value
            # if month <= 0:
            #     year -= 1
            #     month += 12
            # days_in_month = (datetime(year, month + 1, 1) - datetime(year, month, 1)).days
            # return (search_datetime_ob - timedelta(days=days_in_month)).isoformat()

    # Attempt to parse the publication_date with different date formats
    date_formats = [
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO format
        "%d.%m.%Y",  # from whatjobs
        "%Y-%m-%dT%H:%M:%SZ",  # from themuse
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ]

    for date_format in date_formats:
        try:
            date_obj = datetime.strptime(publication_date, date_format)
            return date_obj.isoformat()
        except ValueError:
            pass  # Try the next format

    # If none of the formats match, raise an exception or return a default value as needed
    raise ValueError("Unable to parse publication_date")

