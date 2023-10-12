from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re
import json
import urllib.parse
from config.constants import JOB_LEVELS, JOB_LOCATIONS

def clean_filename(string, replace = False):
    pattern = "[,!.\-: ]"

    if replace == False:
      filename = re.sub(pattern, "_", string)
    else:
      filename = re.sub(pattern, "", string)

    return filename.lower().replace("__", "_")


def create_file_name(isRaw = False, type = "json"):
    path = "some_path"
    now = clean_filename(datetime.now().isoformat(), True)
    location = clean_filename("Berlin, Germany")
    keyword = clean_filename("Data Engineering")
    file_number = 1
    return f"{path}/linkedin_{'raw_' if isRaw else ''}{now}_{location}_{keyword}_{file_number}.{type}"

def split_lines():
  # text = "\nAbout The PositionWe are looking for a top-tier Financial Data Analyst (m/f/d) to join our team on a permanent position in Berlin / Germany.Your MissionProviding best-in-class financial data for our Machine Learning modelsDriving the processes behind our rigorous approach to data qualityScreening, evaluating, and expanding our data sources, such as covering additional equity universes or including alternative data sourcesIdentifying data issues and driving improvements together with our Machine Learning, Quantitative Finance, and Software Engineering teamsHandling communication with our various data vendors to discuss and resolve any problems with their dataYour SkillsetSeveral years of industry experience working with financial data, focusing specifically on equities and related data such as identifiers, market data, fundamental data, and corporate actions.University degree in Computer Science, Finance, Economics, or a related fieldExcellent communication skills and the ability to coordinate with multiple stakeholders, including our internal Machine Learning and Engineering teams, as well as external global data providers.Great attention to detail and a constant drive to dig deep in order to analyse and understand the root causes of data issuesExperience with financial data providers such as Bloomberg, S&P CapitalIQ, Compustat and MSCIAbility to understand and work with large relational databases using SQLExperienced in writing efficient Python code for performing analysis tasks. Familiar with modern data analysis libraries, such as Pandas, Matplotlib, and other frameworksWhy us?An exciting and involving role with significant impact on shaping the future of AI-based sustainable investingA company culture that is based on a spirit of cooperation and is characterized by a high level of quality awareness, openness and attention to detail in all areas of our workThe opportunity to work in an interdisciplinary team with our experienced quant finance, software engineering, and machine learning expertsFlexible working hours, 28 days paid leave and a competitive salary and equity packageA modern loft office in Berlin, Prenzlauer Berg, healthy food (including coffee, juices, fruit, vegetables, sandwiches) and high-end hardware from Apple Internal workshops, unique learning possibilities across a wide range of domains as well as amazing team eventsBenefits such as Urban Sports Club or Fitness First as well as corporate benefitsAbout UsWe are a deep tech pioneer, providing AI-based investment solutions to our clients. Building on the best practices of quantitative asset management we tap the potential of machine learning for sustainable investments on capital market.Our interdisciplinary team consists of experts in the fields of finance, computer science, software engineering, machine learning as well as mathematics, physics, and neuroscience. Ultramarin is embedded in the global AI community, through a close exchange with leading universities and as a member of Inquire Europe.To date Ultramarin is one of the leading drivers for AI-based analyses and decision-making processes in asset management. Ultramarin has been founded in 2017, is headquartered in Berlin, with additional locations in Frankfurt and Munich,and is backed by leading international business angels and VCs.\n      "
  text = "aboutWhatever \nAndthisAnother Example"

  # Find patterns of "WordWord" and replace with "Word\nWord"
  # corrected_text = re.sub(r'([a-z])([A-Z])', r'\1\n\2', text)
  corrected_text = re.sub(r'(\w)([A-Z])', r'\1\n\2', text)
  print(corrected_text)

def split_bullet_list():
  list_text = '''
  - Industry experience working with financial data, particularly equities and related data
  - Knowledge of data quality processes and ability to drive improvements
  - Experience in screening, evaluating, and expanding data sources
  - Excellent communication skills and ability to coordinate with multiple stakeholders
  - Attention to detail and ability to analyze data issues
  - Familiarity with financial data providers such as Bloomberg, S&P CapitalIQ, Compustat, and MSCI
  - Proficiency in SQL and working with large relational databases
  - Proficient in Python and ability to write efficient code for analysis tasks
  - Familiarity with data analysis libraries such as Pandas and Matplotlib
  '''

  skills = [skill.strip() for skill in list_text.strip().split("-") if skill != '']

  print(skills)

def print_cluster(cluster_num=1):
  path = "json_files/linkedin_json_files/jobs_clustered.json"
  with open(path, 'r') as file:
    data = json.load(file)

  for record in data:
    if record.get("cluster") == cluster_num:
      skills = record["job_description_skills"]
      for skill in skills:
        print('- ', skill)
      print('------')


from datetime import datetime

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
      if job_level in level_categories:
          return JOB_LEVELS[level]

      for category in level_categories:
          if category in job_title:
              return JOB_LEVELS[level]

  return JOB_LEVELS["unknown"]

if __name__ == "__main__":
    # file_name = create_file_name()
    # print(file_name)
    # split_lines()
    # split_bullet_list()
    # print_cluster(0)

    # Example usage:
    # search_datetime = "2023-09-27T15:30:00.123456"
    # publication_date = "04.09.2023"  # Can be in various formats
    # publication_date = "2023-01-19T02:58:02Z"  # Can be in various formats
    # publication_date = "Heute"  # Can be in various formats

    # iso_date = transform_to_isoformat(publication_date, search_datetime)
    # print("ISO 8601 Format:", iso_date)

    # print(urllib.parse.quote("ablabla ablabla blabla"))
    # categories_str = "&".join([urllib.parse.quote(cat) for cat in ["erstes wort", "zweites wort"]])
    # print(categories_str)
    # print(transform_job_level('', 'Junior Data Engineer'))
    print(transform_to_isoformat('53 minutes ago', '2023-10-04T14:02:15.129618'))