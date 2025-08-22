import re

from playwright.async_api import TimeoutError

cleaned_links = []

async def scrape_youtube_links(url: str, browser, page):
        # page = await browser.new_page()
        try:
            # Block images and CSS
            async def block_images_and_css(route, request):
                if request.resource_type in ["image", "stylesheet"]:
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", block_images_and_css)

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
                link for link in links if re.match(r"^https://www\.youtube\.com/watch\?", link)
            ]

            return youtube_links

        except TimeoutError:
            print(f"Timeout while loading {url}")
            return []
        except Exception as e:
            print(f"Error while scraping {url}: {str(e)}")
            return []
        # finally:
            # await browser.close()
            # await page.close()