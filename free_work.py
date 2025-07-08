import asyncio
import json
import argparse
from datetime import datetime
from pymongo import MongoClient
from rnet import Impersonate, Client
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, parse_qs, urlencode

def clean_html(html_text):
    """Remove HTML tags and clean up text, preserving essential content without truncation."""
    if not html_text:
        return 'N/A'
    
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    
    # Extract text and clean up
    text = soup.get_text(separator=' ')
    
    # Remove excessive whitespace and newlines
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text if text else 'N/A'

def init_db():
    """Initialize MongoDB connection."""
    """ mongo_uri = "mongodb://localhost:27017"
    mongo_db = "linkedin"
    mongo_collection = "freework" """
    mongo_uri="mongodb+srv://redsdz:Foot199407%40%23@cluster0.ypgrqjo.mongodb.net/linkedin?retryWrites=true&w=majority"
    mongo_db="scraping"
    mongo_collection="freework"
        
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    return client, collection

def insert_job(collection, job):
    """Insert or update a job in the MongoDB collection."""
    collection.replace_one(
        {'id': job['id']},  # Match by job ID
        job,                # Insert/update the entire job document
        upsert=True         # Insert if it doesn't exist
    )

def parse_job_postings(json_data, current_date):
    """Parse JSON response and extract essential job posting details, including skills one by one."""
    job_listings = []
    
    # Affiche la date et l'heure actuelles
    current_time = datetime(2025, 7, 5, 10, 45).strftime("%I:%M %p CEST on %A, %B %d, %Y")
    print(f"Date et heure actuelles : {current_time}\n")
    
    # Ensure the JSON has the expected structure
    if not isinstance(json_data, dict) or 'hydra:member' not in json_data:
        print("Error: Invalid JSON structure or no 'hydra:member' found.")
        return job_listings, None
    
    for job in json_data['hydra:member']:
        # Afficher les compétences une par une
        print(f"Annonce: {job.get('title', 'N/A')} (ID: {job.get('id', 'N/A')})")
        skills = job.get('skills', [])
        if skills:
            print("Compétences trouvées :")
            for skill in skills:
                print(f"- {skill.get('name', 'N/A')}")
        else:
            print("Aucune compétence trouvée dans le champ skills.")
        print()  # Ligne vide pour la lisibilité
        
        # Construct daily_salary from minDailySalary and maxDailySalary if dailySalary is null
        daily_salary = job.get('dailySalary', None)
        if not daily_salary:
            min_salary = job.get('minDailySalary', None)
            max_salary = job.get('maxDailySalary', None)
            if min_salary and max_salary:
                daily_salary = f"{min_salary}-{max_salary} €"
            elif min_salary:
                daily_salary = f"{min_salary} €"
            elif max_salary:
                daily_salary = f"{max_salary} €"
            else:
                daily_salary = 'N/A'
        
        # Extract skills as array of {slug, descriptions} objects
        skills_data = []
        for skill in job.get('skills', []):
            skill_jobs = skill.get('skillJobs', [])
            skill_entry = {
                'slug': skill.get('slug', 'N/A'),
                'descriptions': [skill_job.get('description', 'N/A') for skill_job in skill_jobs] if skill_jobs else ['N/A']
            }
            skills_data.append(skill_entry)
        
        # Construct job URL, replacing /job_postings/ with /job-mission/
        name_for_user_slug = job.get('job', {}).get('nameForUserSlug', '')
        job_id = job.get('@id', '')
        if job_id.startswith('/job_postings/'):
            job_id = job_id.replace('/job_postings/', '/job-mission/')
        job_url = f"https://www.free-work.com/fr/tech-it/{name_for_user_slug}{job_id}" if name_for_user_slug and job_id else 'N/A'
        
        # Extract essential fields, handling missing or null values
        job_details = {
            'id': job.get('id', None),
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
            'url': job_url
        }
        job_listings.append(job_details)
    
    # Get the next page URL, if available
    next_page = json_data.get('hydra:view', {}).get('hydra:next', None)
    
    # Normalize next_page URL parameters to match base_url format
    if next_page:
        parsed_url = urlparse(next_page)
        query_params = parse_qs(parsed_url.query)
        page = query_params.get('page', [''])[0]
        items_per_page = query_params.get('itemsPerPage', ['1000'])[0]
        normalized_query = urlencode({'page': page, 'itemsPerPage': items_per_page})
        next_page = f"/api/job_postings?{normalized_query}"
    
    print(f"Processed {len(job_listings)} jobs for current page")
    return job_listings, next_page

async def fetch_jobs(rnet_client, url, headers, payload, max_retries=3):
    """Fetch jobs from the given URL with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = await rnet_client.get(url, headers=headers, data=payload)
            if resp.status == 302:
                redirect_url = resp.headers.get('Location', 'Unknown')
                print(f"HTTP 302 Redirect for URL {url}: Redirecting to {redirect_url}")
                if attempt < max_retries:
                    print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                return None
            if resp.status != 200:
                print(f"HTTP Error {resp.status} for URL {url}: {await resp.text()[:100]}...")
                if attempt < max_retries:
                    print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                return None
            
            response_text = await resp.text()
            if not response_text.strip():
                print(f"Empty response for URL {url}")
                return None
            
            json_data = json.loads(response_text)
            return json_data
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for URL {url}: {e}")
            print(f"Response content: {response_text[:100]}...")
            if attempt < max_retries:
                print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                await asyncio.sleep(10)
                continue
            return None
        except Exception as e:
            print(f"Error fetching data for URL {url}: {e}")
            if attempt < max_retries:
                print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                await asyncio.sleep(10)
                continue
            return None
    return None

async def main(all_pages=False):
    base_url = "https://www.free-work.com/api/job_postings"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:140.0) Gecko/20100101 Firefox/140.0',
        'Accept': 'application/ld+json',
        'Accept-Language': 'fr',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Referer': 'https://www.free-work.com/fr/tech-it/jobs',
        'x-requested-with': 'XMLHttpRequest',
        'x-varnish-public': '1',
        'sentry-trace': '3c3641bb8fc94ade9d599a5d87bdb0ac-8e7698169182f85e',
        'baggage': 'sentry-environment=production,sentry-public_key=095ab97f02b34d54886e285685ac53e8,sentry-trace_id=3c3641bb8fc94ade9d599a5d87bdb0ac',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Connection': 'keep-alive',
    }
    payload = {}
    
    # Initialize MongoDB connection
    mongo_client, collection = init_db()
    
    try:
        mongo_client.admin.command('ping')  # Test connection
        print("Connected to MongoDB successfully.")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        mongo_client.close()
        return []

    # Initialize rnet Client
    rnet_client = Client(impersonate=Impersonate.Firefox136)
    all_job_listings = []
    
    # Current date for the 'date' field
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        if all_pages:
            # Scrape pages 1 to 10 with itemsPerPage=1000
            current_page = 1
            max_pages = 10
            current_url = f"{base_url}?page=1&itemsPerPage=1000"
            
            while current_page <= max_pages:
                print(f"Fetching page {current_page}...")
                json_data = await fetch_jobs(rnet_client, current_url, headers, payload)
                if not json_data:
                    print(f"Stopping pagination at page {current_page} due to fetch error")
                    break
                
                job_listings, next_page = parse_job_postings(json_data, current_date)
                all_job_listings.extend(job_listings)
                
                # Insert jobs into MongoDB
                for job in job_listings:
                    insert_job(collection, job)
                
                print(f"Inserted {len(job_listings)} jobs from page {current_page}")
                
                if not next_page or current_page == max_pages:
                    break
                
                current_url = f"https://www.free-work.com{next_page}"
                current_page += 1
                
                # Delay to avoid rate-limiting
                await asyncio.sleep(2)
        else:
            # Scrape only page 1 with itemsPerPage=100
            current_url = f"{base_url}?page=1&itemsPerPage=100"
            json_data = await fetch_jobs(rnet_client, current_url, headers, payload)
            if json_data:
                job_listings, _ = parse_job_postings(json_data, current_date)
                all_job_listings.extend(job_listings)
                
                # Insert jobs into MongoDB
                for job in job_listings:
                    insert_job(collection, job)
                
                print(f"Inserted {len(job_listings)} jobs from page 1")
        
        print(f"Total jobs inserted: {len(all_job_listings)}")
        return all_job_listings
    finally:
        # Close MongoDB connection
        mongo_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape job postings from Free-Work")
    parser.add_argument('--all', type=lambda s: s.lower() == 'true', default=False,
                        help="Set to 'true' to scrape pages 1-10 with 1000 items per page, 'false' for page 1 with 100 items")
    args = parser.parse_args()
    
    asyncio.run(main(all_pages=args.all))