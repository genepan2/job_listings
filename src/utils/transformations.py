from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re
from config.constants import JOB_LEVELS, JOB_LOCATIONS


def transform_job_level(job_level: str = 'Entry', job_title: str = None):
  job_level = job_level.lower().strip()
  job_title = job_title.lower().strip()

  job_levels_mapping = {
      "Intern": ["internship", "intern", "working student"],
      "Entry": ["entry level", "entry", "junior"],
      "Middle": ["mid level", "mid", "not applicable"],
      "Senior": ["senior level", "senior"],
      "Lead": ["lead"],
      "Head": ["head"],
      "Student": ["Working Student", "student"],

  }

  for level, level_categories in job_levels_mapping.items():
      if job_level in level_categories:
          return JOB_LEVELS[level]

      for category in level_categories:
          if category in job_title:
              return JOB_LEVELS[level]

  return JOB_LEVELS["Middle"]

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


def transform_to_isoformat(publication_date, search_datetime):
    today_names = ["today", "heute"]
    yesterday_names = ["yesterday", "gestern"]

    # Convert search_datetime to a datetime object at the start
    search_datetime_ob = datetime.strptime(search_datetime, '%Y-%m-%dT%H:%M:%S.%f')

    # Check if publication_date is a special keyword like "today" or "yesterday"
    if publication_date and publication_date.lower() in today_names or publication_date is None:
        return search_datetime

    if publication_date and publication_date.lower() in yesterday_names:
        new_date_time_obj = search_datetime_ob - timedelta(days=1)
        return new_date_time_obj.isoformat()

    # Use regular expression to extract the numeric value and unit (hour, day, week, month)
    print(publication_date)
    if publication_date and isinstance(publication_date, str):  # Add this check
        match = re.match(r'(\d+)\s+(\w+)', publication_date)
        if match:
            value, unit = match.groups()
            value = int(value)
            unit = unit.lower()

            unit = unit[:-1] if unit.endswith('s') else unit

            if unit == "second" :
                return (search_datetime_ob - timedelta(seconds=value)).isoformat()
            elif unit == "minute" :
                return (search_datetime_ob - timedelta(minutes=value)).isoformat()
            elif unit == "hour" :
                return (search_datetime_ob - timedelta(hours=value)).isoformat()
            elif unit == "day":
                return (search_datetime_ob - timedelta(days=value)).isoformat()
            elif unit == "week":
                return (search_datetime_ob - timedelta(weeks=value)).isoformat()
            elif unit == "month":
                return (search_datetime_ob - relativedelta(months=value)).isoformat()

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

def transform_job_title(title: str):
    to_remove_genders = ["(f/m/x)", "(m/f/d)", "(f/m/d)", "(m/w/d)", "(w/m/d)", "(M/W/D)", "m/f/d", "(w/m/x)", "(all genders!)", "(all genders)", "(All Genders)"]
    to_remove_level = ["(Junior)", "Junior", "(Senior)", "Senior", "Intern", "Working Student"]

    # Remove gender-related phrases
    title_parts = [part for part in title.split() if part not in to_remove_genders]
    # Remove level-related phrases
    title_parts = [part for part in title_parts if part not in to_remove_level]

    # Join the remaining parts back into a string
    cleaned_title = ' '.join(title_parts)

    # Remove extra spaces and strip
    cleaned_title = ' '.join(cleaned_title.split())

    return cleaned_title



