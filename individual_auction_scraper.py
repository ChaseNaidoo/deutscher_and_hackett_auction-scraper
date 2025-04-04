import asyncio
import json
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def scrape_lot_details(crawler, lot_url):
    """Scrapes details from an individual lot page."""
    try:
        if lot_url.startswith("/"):
            lot_url = f"https://www.deutscherandhackett.com{lot_url}"

        await asyncio.sleep(1)  # Delay to avoid rate-limiting
        result = await crawler.arun(url=lot_url)  # Adjust js_execution if needed
        if not result.success:
            logging.error(f"Failed to crawl lot page {lot_url}")
            return None

        soup = BeautifulSoup(result.html, "html.parser")

        artist_div = soup.find("div", class_="field-name-field-lot-artist")
        artist = artist_div.find("p").get_text(strip=True) if artist_div and artist_div.find("p") else "Unknown Artist"

        title_div = soup.find("div", class_="field-lot-title")
        artwork_title = title_div.get_text(strip=True) if title_div else "Unknown Title"

        medium_div = soup.find("div", class_="field-name-field-lot-medium")
        medium = medium_div.find("p").get_text(strip=True) if medium_div and medium_div.find("p") else "Unknown Medium"

        size_div = soup.find("div", class_="field-name-field-lot-size")
        size = size_div.find("p").get_text(strip=True) if size_div and size_div.find("p") else "Not specified"

        signed_div = soup.find("div", class_="field-name-field-lot-signed")
        signed = signed_div.find("p").get_text(strip=True) if signed_div and signed_div.find("p") else "No signage found"

        provenance_div = soup.find("div", class_="field-name-field-lot-provenance")
        provenance = provenance_div.find("p").get_text(strip=True) if provenance_div and provenance_div.find("p") else "Not specified"

        condition_div = soup.find("div", class_="field-name-field-lot-condition")
        condition = condition_div.find("p").get_text(strip=True) if condition_div and condition_div.find("p") else "Not specified"

        sold_price_div = soup.find("div", class_="field-price-sold")
        sold_price = sold_price_div.get_text(strip=True).split("in")[0].replace("Sold for", "").strip() if sold_price_div else None

        if not sold_price:
            return None

        return {
            "artist": artist,
            "title": artwork_title,
            "medium": medium,
            "size": size,
            "signage": signed,
            "provenance": provenance,
            "condition": condition,
            "price": sold_price,
            "url": lot_url
        }

    except Exception as e:
        logging.error(f"Error scraping lot details for {lot_url}: {str(e)}")
        return None

async def scrape_auction_details(crawler, auction):
    """Scrapes lot details from an auction page and follows sold lot links."""
    try:
        if auction["lots"]:
            logging.info(f"Skipping already processed auction: {auction['url']}")
            return True

        result = await crawler.arun(url=auction["url"])
        if not result.success:
            logging.error(f"Failed to crawl auction page {auction['url']}")
            return False

        soup = BeautifulSoup(result.html, "html.parser")
        lot_rows = soup.select("div.views-row")
        logging.info(f"Found {len(lot_rows)} total lots on page for auction: {auction['url']}")

        tasks = []
        sold_count = 0
        for row in lot_rows:
            sold_price_div = row.find("div", class_="field-price-sold")
            if sold_price_div:
                sold_count += 1
                lot_link = row.find("a", href=lambda h: h and "/auction/lot/" in h)
                if lot_link:
                    tasks.append(scrape_lot_details(crawler, lot_link["href"]))
                else:
                    logging.warning(f"Sold item found but no lot link in row for auction: {auction['url']}")
        logging.info(f"Identified {sold_count} sold lots for auction: {auction['url']}")

        lot_details = await asyncio.gather(*tasks)
        auction["lots"] = [
            {**lot, "auctionUrl": auction["url"]}
            for lot in lot_details if lot is not None
        ]
        logging.info(f"Scraped {len(auction['lots'])} lots for auction: {auction['url']}")
        if sold_count != len(auction["lots"]):
            logging.warning(f"Mismatch: Identified {sold_count} sold lots, but only scraped {len(auction['lots'])}")
        return True

    except Exception as e:
        logging.error(f"Error scraping auction details for {auction['url']}: {str(e)}")
        return False

def save_progress(auctions_data, filename="single_auction.json"):
    """Saves the current state of auctions_data to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(auctions_data, f, indent=4)
    logging.info(f"Progress saved to {filename}")

async def main():
    # Define the specific auction to scrape (e.g., the last incomplete one)
    auction_url = "https://www.deutscherandhackett.com/81-important-australian-indigenous-art"
    output_file = "single_auction.json"

    # Create a single auction dictionary
    auction = {
        "url": auction_url,
        "title": "Beyond Sacred: Aboriginal Art, The Laverty Collection",  # Optional, can be scraped if needed
        "year": "2015",  # Optional
        "location": "Sydney",  # Optional
        "date": "8 March 2015",  # Optional
        "sale_number": "38",  # Optional
        "lots": []
    }
    auctions_data = [auction]  # List with one auction

    async with AsyncWebCrawler() as crawler:
        # Process only this auction
        success = await scrape_auction_details(crawler, auction)
        if success:
            logging.info(f"Successfully processed auction: {auction['url']}")
        save_progress(auctions_data, output_file)

        logging.info(f"Scraping complete. Processed 1 auction. Data saved in {output_file}")

if __name__ == "__main__":
    asyncio.run(main())