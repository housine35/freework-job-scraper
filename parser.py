import re
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode
from utils import clean_html

def search_scraping(description, candidate_profile):
    """
    Search for the exact terms 'scraping' or 'web scraping' in the description and candidate profile.
    Returns True if found, otherwise False.
    """
    if not description:
        description = ""
    if not candidate_profile:
        candidate_profile = ""

    description = description.lower()
    candidate_profile = candidate_profile.lower()
    
    if 'scraping' in description or 'scraping' in candidate_profile:
        if 'web scraping' in description or 'web scraping' in candidate_profile:
            return True
    return False


def parse_job_postings(json_data, current_date):
    """
    Parse the job data from JSON and return a list of job objects and the next page URL if available.
    """
    job_listings = []

    current_time = datetime.now().strftime("%I:%M %p CEST on %A, %B %d, %Y")

    if not isinstance(json_data, dict) or 'hydra:member' not in json_data:
        print("Error: Invalid JSON structure or missing 'hydra:member'.")
        return job_listings, None

    for job in json_data['hydra:member']:
        skills = job.get('skills', [])
        if skills:
            for skill in skills:
                skill.get('name', 'N/A')

        daily_salary = job.get('dailySalary', None)
        if not daily_salary:
            min_salary = job.get('minDailySalary')
            max_salary = job.get('maxDailySalary')
            if min_salary and max_salary:
                daily_salary = f"{min_salary}-{max_salary} €"
            elif min_salary:
                daily_salary = f"{min_salary} €"
            elif max_salary:
                daily_salary = f"{max_salary} €"
            else:
                daily_salary = 'N/A'

        skills_data = []
        for skill in skills:
            skill_jobs = skill.get('skillJobs', [])
            skill_entry = {
                'slug': skill.get('slug', 'N/A'),
                'descriptions': [s.get('description', 'N/A') for s in skill_jobs] if skill_jobs else ['N/A']
            }
            skills_data.append(skill_entry)

        job_id = job.get('@id', '')
        if job_id.startswith('/job_postings/'):
            job_id = job_id.replace('/job_postings/', '/job-mission/')
        name_for_user_slug = job.get('job', {}).get('nameForUserSlug', '')
        job_url = f"https://www.free-work.com/fr/tech-it/{name_for_user_slug}{job_id}" if name_for_user_slug and job_id else 'N/A'

        # Détection du scraping
        scraping_detected = search_scraping(
            job.get('description', ''),
            job.get('candidateProfile', '')
        )

        job_details = {
            'id': job.get('id'),
            'title': job.get('title', 'N/A'),
            'location': job.get('location', {}).get('label', 'N/A'),
            'company': job.get('company', {}).get('name', 'N/A'),
            'description': clean_html(job.get('description', 'N/A')),
            'candidate_profile': clean_html(job.get('candidateProfile', 'N/A')),
            'skills': skills_data,
            'experience_level': job.get('experienceLevel', 'N/A'),
            'duration': f"{job.get('durationValue', 'N/A')} {job.get('durationPeriod', 'N/A')}" if job.get('durationValue') else 'N/A',
            'remote_mode': job.get('remoteMode', 'N/A'),
            'daily_salary': daily_salary,
            'starts_at': job.get('startsAt', 'N/A'),
            'expired_at': job.get('expiredAt', 'N/A'),
            'published_at': job.get('publishedAt', 'N/A'),
            'contracts': job.get('contracts', []),
            'source': 'freework',
            'date': current_date,
            'url': job_url,
            'scraping': scraping_detected  # Ajout du champ 'scraping'
        }

        job_listings.append(job_details)

    next_page = json_data.get('hydra:view', {}).get('hydra:next', None)
    if next_page:
        parsed_url = urlparse(next_page)
        query_params = parse_qs(parsed_url.query)
        page = query_params.get('page', [''])[0]
        items_per_page = query_params.get('itemsPerPage', ['1000'])[0]
        normalized_query = urlencode({'page': page, 'itemsPerPage': items_per_page})
        next_page = f"/api/job_postings?{normalized_query}"

    print(f"Processed {len(job_listings)} jobs on this page")
    return job_listings, next_page
