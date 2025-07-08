import asyncio
from datetime import datetime
import argparse
from scraper import fetch_jobs
from parser import parse_job_postings
from db import init_db, insert_job
from rnet import Client, Impersonate

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

    mongo_client, collection = init_db()
    try:
        mongo_client.admin.command('ping')
        print("MongoDB connection successful.")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        mongo_client.close()
        return []

    rnet_client = Client(impersonate=Impersonate.Firefox136)
    all_jobs = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    try:
        if all_pages:
            current_page = 1
            max_pages = 10
            current_url = f"{base_url}?page=1&itemsPerPage=1000"

            while current_page <= max_pages:
                print(f"Fetching page {current_page}...")
                json_data = await fetch_jobs(rnet_client, current_url, headers, payload)
                if not json_data:
                    print("Stopping due to error.")
                    break

                job_listings, next_page = parse_job_postings(json_data, current_date)
                all_jobs.extend(job_listings)

                for job in job_listings:
                    insert_job(collection, job)

                print(f"Inserted {len(job_listings)} jobs from page {current_page}.")

                if not next_page or current_page == max_pages:
                    break

                current_url = f"https://www.free-work.com{next_page}"
                current_page += 1
                await asyncio.sleep(2)

        else:
            current_url = f"{base_url}?page=1&itemsPerPage=100"
            json_data = await fetch_jobs(rnet_client, current_url, headers, payload)
            if json_data:
                job_listings, _ = parse_job_postings(json_data, current_date)
                all_jobs.extend(job_listings)
                for job in job_listings:
                    insert_job(collection, job)

        print(f"Total jobs inserted: {len(all_jobs)}")
        return all_jobs
    finally:
        mongo_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Free-Work Job Scraper")
    parser.add_argument('--all', type=lambda s: s.lower() == 'true', default=False,
                        help="Set to 'true' to scrape 10 pages, or 'false' for one page.")
    args = parser.parse_args()
    asyncio.run(main(all_pages=args.all))
