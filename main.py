import asyncio
from db.database import DatabaseManager
from scraper.scraper import WilayahSekolahScraper
from playwright.async_api import async_playwright

async def run_scraping():
    db_manager = DatabaseManager('sql/wilayah.db')
    scraper = WilayahSekolahScraper(db_manager)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        await scraper.scrape_wilayah(page)
        await scraper.scrape_kabupaten(page)
        await scraper.scrape_kecamatan(page)
        await scraper.scrape_sekolah(page)

        await browser.close()
        db_manager.close()

    print("Scraping selesai.")

if __name__ == '__main__':
    asyncio.run(run_scraping())
