import asyncio
from datetime import datetime
import argparse
import sys
from scraper import fetch_jobs
from parser import parse_job_postings
from db import init_db, insert_job
from rnet import Emulation, Client


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

    try:
        mongo_client, collection = init_db()
        print("MongoDB connection successful.")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        sys.exit(1)

    # You can still create an Rnet Client if you want,
    # but it's not required for fetch_jobs() anymore.
    rnet_client = Client(emulation=Emulation.Firefox143)

    all_jobs = []
    current_date = datetime.now().strftime('%Y-%m-%d')

    try:
        if all_pages:
            current_page = 1
            max_pages = 10
            current_url = f"{base_url}?page=1&itemsPerPage=1000"

            while current_page <= max_pages:
                print(f"Fetching page {current_page}...")
                json_data = await fetch_jobs(current_url, headers)
                if not json_data:
                    print("Stopping due to error in fetch_jobs.")
                    break

                try:
                    job_listings, next_page = parse_job_postings(json_data, current_date)
                except Exception as e:
                    print(f"Error parsing job postings: {e}")
                    break

                all_jobs.extend(job_listings)
                for job in job_listings:
                    try:
                        insert_job(collection, job)
                    except Exception as e:
                        print(f"Error inserting job: {e}")
                        continue

                print(f"Inserted {len(job_listings)} jobs from page {current_page}.")

                if not next_page or current_page == max_pages:
                    break

                current_url = f"https://www.free-work.com{next_page}"
                current_page += 1
                await asyncio.sleep(2)

        else:
            current_url = f"{base_url}?page=1&itemsPerPage=100"
            json_data = await fetch_jobs(current_url, headers)
            if json_data:
                try:
                    job_listings, _ = parse_job_postings(json_data, current_date)
                    all_jobs.extend(job_listings)
                    for job in job_listings:
                        insert_job(collection, job)
                except Exception as e:
                    print(f"Error processing single page: {e}")
                    sys.exit(1)

        print(f"Total jobs inserted: {len(all_jobs)}")
        return all_jobs

    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        mongo_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Free-Work Job Scraper")
    parser.add_argument('--all', type=lambda s: s.lower() == 'true', default=False,
                        help="Set to 'true' to scrape 10 pages, or 'false' for one page.")
    args = parser.parse_args()

    try:
        asyncio.run(main(all_pages=args.all))
    except Exception as e:
        print(f"Main execution failed: {e}")
        sys.exit(1)
