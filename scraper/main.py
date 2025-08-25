import asyncio
import os
import re
import signal
import clean_links
import db
from test_connection import check_connection
from playwright.async_api import async_playwright, TimeoutError

async def youtube_scraper(db_ready_event: asyncio.Event, task_id: int):
    await db_ready_event.wait()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
        )
        page = await browser.new_page()

        # Block images and CSS for speed
        async def block_images_and_css(route, request):
            if request.resource_type in {"image", "stylesheet"}:
                await route.abort()
            else:
                await route.continue_()
        await page.route("**/*", block_images_and_css)

        while True:
            if not check_connection():
                print(f"[Scraper {task_id}] No internet. Retrying in 3s...")
                await asyncio.sleep(3)
                continue

            try:
                video_id = db.grab_link()
                if not video_id:
                    print(f"[Scraper {task_id}] No more links to scrape. Waiting...")
                    await asyncio.sleep(5)
                    continue

                url = f"https://www.youtube.com/watch?v={video_id}"
                links = await scrape_youtube_links(page, url)

                # Deduplicate and clean
                cleaned = {clean_links.extract_youtube_id(link) for link in links}
                cleaned = {link for link in cleaned if link}

                if cleaned:
                    db.save_link(list(cleaned))
                    count = db.count_links()
                    print(f"[Scraper {task_id}] Saved {len(cleaned)} links. DB total: {count}")

            except Exception as e:
                print(f"[Scraper {task_id}] Error: {e}")
                await asyncio.sleep(2)


async def scrape_youtube_links(page, url: str):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
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
        return youtube_links

    except TimeoutError:
        print(f"Timeout while loading {url}")
        return []
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return []


async def main():
    db_ready_event = asyncio.Event()

    # Graceful shutdown support
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

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

    num_scrapers = int(os.getenv("NUM_SCRAPERS", 4))
    tasks = [asyncio.create_task(youtube_scraper(db_ready_event, i)) for i in range(num_scrapers)]

    await stop_event.wait()
    print("Shutting down scrapers...")

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
