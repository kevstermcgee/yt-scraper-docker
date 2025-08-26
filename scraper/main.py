import asyncio
import os
import random
import re
import signal
import clean_links
import db
from test_connection import check_connection
from playwright.async_api import async_playwright, TimeoutError

async def youtube_scraper(db_ready_event: asyncio.Event, task_id: int, page):
    await db_ready_event.wait()
    while True:
        if not check_connection():
            print(f"[Scraper {task_id}] No internet. Retrying in 2s...")
            await asyncio.sleep(2)
            continue
        try:
            video_ids = db.grab_links_batch(5)  # Grab 5 links at a time
            if not video_ids:
                await asyncio.sleep(2)
                continue

            # Scrape all links concurrently
            tasks = []
            for vid in video_ids:
                tasks.append(scrape_youtube_links(await page.context.new_page(),f"https://www.youtube.com/watch?v={vid}"))
            results = await asyncio.gather(*tasks)

            # Flatten, clean, and deduplicate
            all_links = {clean_links.extract_youtube_id(link)
                         for sublist in results for link in sublist if link}

            await asyncio.sleep(random.uniform(2, 5)) # Delay between requests

            if all_links:
                print(list(all_links))
                db.save_link(list(all_links))  # Batch save
            total_links_in_db = db.count_links()
            print(f"Database contains {total_links_in_db} links.")

        except Exception as e:
            print(f"[Scraper {task_id + 1}] Error: {e}")
            await asyncio.sleep(2)


async def scrape_youtube_links(page, url: str):
    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_selector("ytd-watch-flexy", timeout=60000)

        # Grab only video links
        links = await page.eval_on_selector_all(
            "a[href^='/watch?v=']",
            "elements => elements.map(el => el.href)"
        )

        youtube_links = [
            link for link in links
            if re.match(r"^https:\/\/(www\.)?youtube\.com\/watch\?v=", link)
        ]
        await page.close()
        return youtube_links

    except TimeoutError:
        print(f"Timeout while loading {url}")
        return []
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return []


async def main():
    db_ready_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    number_of_contexts = 5  # Number of scrapers to run concurrently

    def shutdown():
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    # DB setup first
    print("Waiting for database connection...")
    db.wait_for_db()
    db.ensure_table_exists()
    print("Database ready.")
    db_ready_event.set()

    # num_scrapers = int(os.getenv("NUM_SCRAPERS", 1)) # change to have more browser instances running.
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--disable-blink-features=AutomationControlled",  # Important for stealth
                "--no-first-run",
                "--disable-default-apps",
                "--disable-extensions",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
        )

        contexts = [await browser.new_context() for _ in range(number_of_contexts)]
        pages = [await ctx.new_page() for ctx in contexts]

        # Block images and CSS for speed
        async def block_elements(route, request):
            if request.resource_type in {"image", "stylesheet", "font", "media"}:
                await route.abort()
            else:
                await route.continue_()

        for page in pages:
            await page.route("**/*", block_elements)

        # Launch scraper tasks
        tasks = [asyncio.create_task(youtube_scraper(db_ready_event, i, pages[i]))
                 for i in range(number_of_contexts)]

        # Wait for shutdown signal while scrapers run
        await stop_event.wait()
        print("Shutting down scrapers...")

        # Cancel all scraper tasks
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Close browser after all tasks finished
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
