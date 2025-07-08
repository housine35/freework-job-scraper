import asyncio
import json
from rnet import Client, Impersonate

async def fetch_jobs(rnet_client, url, headers, payload, max_retries=3):
    """
    Send an HTTP GET request to the given URL and return parsed JSON response.
    Retries on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = await rnet_client.get(url, headers=headers, data=payload)
            if resp.status == 302:
                redirect_url = resp.headers.get('Location', 'Unknown')
                print(f"HTTP 302 redirect to {redirect_url}")
                if attempt < max_retries:
                    print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                return None

            if resp.status != 200:
                print(f"HTTP Error {resp.status}: {await resp.text()[:100]}...")
                if attempt < max_retries:
                    print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                return None

            response_text = await resp.text()
            if not response_text.strip():
                print("Empty response received.")
                return None

            return json.loads(response_text)

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            if attempt < max_retries:
                print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                await asyncio.sleep(10)
                continue
            return None

        except Exception as e:
            print(f"Error fetching data: {e}")
            if attempt < max_retries:
                print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                await asyncio.sleep(10)
                continue
            return None

    return None
