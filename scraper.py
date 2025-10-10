import asyncio
import json
import rnet
from rnet import Method

async def fetch_jobs(url, headers, max_retries=3):
    """
    Send an HTTP GET request using Rnet and return parsed JSON response.
    Retries on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp: rnet.Response = await rnet.request(
                Method.GET,
                url=url,
                headers=headers
            )

            # Extract numeric status code safely
            try:
                status_code = int(str(resp.status).split()[0])
            except Exception:
                status_code = 0

            print(f"Status Code: {resp.status}")

            if status_code == 302:
                redirect_url = resp.headers.get("Location", "Unknown")
                print(f"HTTP 302 redirect to {redirect_url}")
                if attempt < max_retries:
                    print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                return None

            if status_code != 200:
                text = await resp.text()
                print(f"HTTP Error {resp.status}: {text[:100]}...")
                if attempt < max_retries:
                    print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
                    await asyncio.sleep(10)
                    continue
                return None

            response_text = await resp.text()
            if not response_text.strip():
                print("Empty response received.")
                return None

            # Parse and return JSON content
            return json.loads(response_text)

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        if attempt < max_retries:
            print(f"Retrying ({attempt}/{max_retries}) after 10 seconds...")
            await asyncio.sleep(10)

    return None
