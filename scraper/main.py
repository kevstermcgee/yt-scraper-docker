import asyncio
import os
import re
import clean_links
import db
from test_connection import check_connection
from playwright.async_api import async_playwright, TimeoutError

async def youtube_scraper(db_ready_event: asyncio.Event):
    await db_ready_event.wait()
    final_links = []

    while True:
        if check_connection():
            try:
                video_id = db.grab_link()
                url = "https://www.youtube.com/watch?v=" + video_id
                links = await scrape_youtube_links(url)

                # Process the scraped links
                for link in links:
                    clean_link = clean_links.extract_youtube_id(link)
                    final_links.append(clean_link)

                db.save_link(final_links)

                amount_of_links_in_db = db.count_links()

                print(f"Database contains {amount_of_links_in_db} links.")
            except Exception as e:
                print(f"Error in main loop: {e}")
            else:
                print("No internet. Waiting to retry...")
                await asyncio.sleep(3)


async def scrape_youtube_links(url: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
        )

        page = await browser.new_page()

        # Block images and CSS
        async def block_images_and_css(route, request):
            if request.resource_type in ["image", "stylesheet"]:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", block_images_and_css)
        try:
            # Changed wait_until to 'domcontentloaded' and increased timeout
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Wait for a common YouTube element to be sure the page is loaded
            await page.wait_for_selector("ytd-watch-flexy", timeout=60000)

            # Get all hrefs from anchor tags
            links = await page.eval_on_selector_all(
                "a",
                "elements => elements.map(el => el.href)"
            )

            youtube_links = [
                link for link in links if re.match(r"^https:\/\/(www\.)?youtube\.com\/watch\?v=", link)
            ]

            return youtube_links

        except TimeoutError:
            print(f"Timeout while loading {url}")
            return []
        except Exception as e:
            print(f"Error while scraping {url}: {str(e)}")
            return []


async def main():
    db_ready_event = asyncio.Event()

    # Create and start the scraper tasks
    num_scrapers = int(os.getenv("NUM_SCRAPERS", 4))
    tasks = [asyncio.create_task(youtube_scraper(db_ready_event)) for _ in range(num_scrapers)]

    # Perform all synchronous database setup first
    print("Waiting for database connection...")
    db.wait_for_db()
    db.ensure_table_exists()
    print("Database connection and table creation complete.")

    # Signal to all waiting tasks that the database is ready
    db_ready_event.set()
    print("Notifying scraper tasks to start.")

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())