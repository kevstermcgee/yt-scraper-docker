import asyncio
import os

from playwright.async_api import async_playwright

import clean_links
import db
import get_links
from test_connection import check_connection

async def youtube_scraper(db_ready_event: asyncio.Event):
    print("Scraper task waiting for database to be ready...")
    await db_ready_event.wait()
    print("Scraper task is running...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        queue = []
        while True:
            if check_connection():
                try:
                    video_ids = [] # empty list for storing video ids

                    # if len(queue) == 0:
                        # video_id = db.grab_link()
                        # if video_id is None:
                            # print("No links in database. Waiting...")
                            # await asyncio.sleep(5)
                            # continue

                    # else:
                        # video_id = queue.pop(0)

                    video_id = db.grab_link()

                    url = "https://www.youtube.com/watch?v=" + video_id

                    links = await get_links.scrape_youtube_links(url, browser, page)
                    # print(f"Initial links scraped: {len(links)}")  # New logging

                    # Process the scraped links
                    final_links = []
                    invalid_links = 0  # Counter for invalid links
                    non_11_char_links = 0  # Counter for links that aren't 11 chars
                    for link in links:
                        clean_link = clean_links.extract_youtube_id(link)
                        if clean_link is None:
                            invalid_links += 1
                        elif len(clean_link) != 11:
                            non_11_char_links += 1
                        else:
                            final_links.append(clean_link)

                    # Remove duplicates from the new links and add to the queue
                    new_links_to_save = list(set(final_links))
                    # print(f"Unique links after deduplication: {len(new_links_to_save)}")

                    # duplicates_removed = len(final_links) - len(new_links_to_save)

                    for new_link in new_links_to_save:
                        if len(queue) < 100:
                            queue.append(new_link)

                    # amount_of_found_links = len(new_links_to_save)
                    # print(f"Found {amount_of_found_links} new links")

                    db.save_link(final_links)

                    amount_of_links_in_db = db.count_links()
                    print(f"Database contains {amount_of_links_in_db} links.")

                    video_ids.clear()
                except Exception as e:
                    print(f"Error in main loop: {str(e)}")
            else:
                print("No internet. Waiting to retry...")
                await asyncio.sleep(3)


async def main():
    db_ready_event = asyncio.Event()

    # Create and start the scraper tasks, passing the event to them
    num_scrapers = int(os.getenv("NUM_SCRAPERS", 4))
    tasks = [asyncio.create_task(youtube_scraper(db_ready_event)) for _ in range(num_scrapers)]

    # Perform all synchronous database setup first
    print("Waiting for database connection...")
    db.wait_for_db()
    db.ensure_table_exists()
    print("Database connection and table creation complete.")

    # Signal to all waiting tasks that the database is ready
    db_ready_event.set()
    print("Notifying scraper tasks to begin.")

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())