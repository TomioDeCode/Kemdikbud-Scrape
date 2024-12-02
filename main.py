import os
import asyncio
from dotenv import load_dotenv
from db.database import DatabaseManager
from scraper.scraper import WilayahSekolahScraper
from playwright.async_api import async_playwright


async def run_scraping():
    load_dotenv()

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    db_manager = DatabaseManager(supabase_url, supabase_key)
    scraper = WilayahSekolahScraper(db_manager)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        await scraper.scrape_wilayah(page)
        await scraper.scrape_kabupaten(page)
        await scraper.scrape_kecamatan(page)
        await scraper.scrape_sekolah(page)

        await browser.close()

    print("Scraping selesai.")


if __name__ == "__main__":
    asyncio.run(run_scraping())
