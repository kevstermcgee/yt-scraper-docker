import asyncio
import os
import random
import re
import signal
import clean_links
import db
from test_connection import check_connection
from playwright.async_api import async_playwright, TimeoutError

async def scrape_youtube_links(page, url: str):
    try:
        await page.goto(url, wait_until="networkidle", timeout=100000)
        await page.wait_for_selector("ytd-watch-flexy", timeout=100000)

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
    finally:
        if page and not page.is_closed():
            await page.close()

async def scrape_and_release(semaphore: asyncio.Semaphore, page, url: str):
    try:
        # Perform the actual scraping
        return await scrape_youtube_links(page, url)
    finally:
        # Release the semaphore slot for the next task to use
        semaphore.release()

async def youtube_scraper(db_ready_event: asyncio.Event, task_id: int, context, semaphore: asyncio.Semaphore):
    await db_ready_event.wait()
    while True:
        if not check_connection():
            print(f"[Scraper {task_id}] No internet. Retrying in 2s...")
            await asyncio.sleep(2)
            continue
        try:
            video_ids = db.grab_links_batch(5)
            if not video_ids:
                await asyncio.sleep(2)
                continue

            tasks = []
            for vid in video_ids:
                await semaphore.acquire()
                page = await context.new_page()
                task = asyncio.create_task(scrape_and_release(
                    semaphore, page, f"https://www.youtube.com/watch?v={vid}"
                ))
                tasks.append(task)
            results = await asyncio.gather(*tasks)

            all_links = {clean_links.extract_youtube_id(link)
                         for sublist in results for link in sublist if link}

            if all_links:
                db.save_link(list(all_links))
            total_links_in_db = db.count_links()
            print(f"Database contains {total_links_in_db} links.")

        except Exception as e:
            print(f"[Scraper {task_id + 1}] Error: {e}")
            await asyncio.sleep(2)


async def main():
    db_ready_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    number_of_contexts = 5

    max_concurrent_pages = 15 # Tune this number based on your system's RAM
    semaphore = asyncio.Semaphore(max_concurrent_pages)

    def shutdown():
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    print("Waiting for database connection...")
    db.wait_for_db()
    db.ensure_table_exists()
    print("Database ready.")
    db_ready_event.set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--disable-default-apps",
                "--disable-extensions",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
        )

        contexts = [await browser.new_context() for _ in range(number_of_contexts)]

        async def block_elements(route):
            if route.request.resource_type in {"image", "stylesheet", "font", "media"}:
                await route.abort()
            else:
                await route.continue_()

        for context in contexts:
            await context.route("**/*", block_elements)

        tasks = [asyncio.create_task(youtube_scraper(db_ready_event, i, contexts[i], semaphore))
                 for i in range(number_of_contexts)]

        await stop_event.wait()
        print("Shutting down scrapers...")

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())