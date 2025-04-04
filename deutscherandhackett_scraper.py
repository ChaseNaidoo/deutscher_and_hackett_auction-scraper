import asyncio
import json
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def scrape_past_auctions(crawler, url):
    """Scrapes all auction details from the past auctions page."""
    try:
        result = await crawler.arun(url=url)
        if not result.success:
            logging.error(f"Failed to crawl {url}")
            return []

        soup = BeautifulSoup(result.html, "html.parser")
        auctions_data = []
        year_headings = soup.select("h3")

        for year_heading in year_headings:
            year = year_heading.get_text(strip=True).strip()
            if year.isdigit() and int(year) <= 2014:
                logging.info(f"Stopping at year {year}")
                break

            next_element = year_heading.find_next_sibling()
            while next_element and next_element.name != "h3":
                if next_element.get("class", []) and "views-row" in next_element["class"]:
                    auction_link = next_element.find("a", href=True)
                    if auction_link and "/auctions/past" not in auction_link["href"]:
                        title = auction_link.get_text(strip=True)
                        url = auction_link["href"]
                        if url.startswith("/"):
                            url = f"https://www.deutscherandhackett.com{url}"

                        location_div = next_element.find("div", class_="field-name-field-auction-location")
                        location = location_div.find("div", class_="field-item").get_text(strip=True) if location_div else "No location found"

                        date_div = next_element.find("div", class_="field-name-field-auction-date")
                        date = date_div.find("span", class_="date-display-single").get_text(strip=True) if date_div else "No date found"

                        sale_div = next_element.find("div", class_="field-name-field-auction-number")
                        sale_number = sale_div.find("div", class_="field-item").get_text(strip=True) if sale_div else "No sale number found"

                        auctions_data.append({
                            "url": url,
                            "title": title,
                            "year": year,
                            "location": location,
                            "date": date,
                            "sale_number": sale_number,
                            "lots": []
                        })

                next_element = next_element.find_next_sibling()

        return auctions_data

    except Exception as e:
        logging.error(f"Error scraping {url}: {str(e)}")
        return []

async def scrape_lot_details(crawler, lot_url):
    """Scrapes details from an individual lot page."""
    try:
        if lot_url.startswith("/"):
            lot_url = f"https://www.deutscherandhackett.com{lot_url}"

        # Add a small delay to avoid overwhelming the server
        await asyncio.sleep(1)

        result = await crawler.arun(url=lot_url)
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
        return True

    except Exception as e:
        logging.error(f"Error scraping auction details for {auction['url']}: {str(e)}")
        return False

def save_progress(auctions_data, filename="auctions_with_lots.json"):
    """Saves the current state of auctions_data to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(auctions_data, f, indent=4)
    logging.info(f"Progress saved to {filename}")

async def main():
    base_url = "https://www.deutscherandhackett.com/auctions/past"
    output_file = "auctions_with_lots.json"

    async with AsyncWebCrawler() as crawler:
        if os.path.exists(output_file):
            with open(output_file, "r", encoding="utf-8") as f:
                auctions_data = json.load(f)
            logging.info(f"Loaded existing data with {len(auctions_data)} auctions from {output_file}")
        else:
            auctions_data = await scrape_past_auctions(crawler, base_url)
            logging.info(f"Scraped {len(auctions_data)} auctions from {base_url}")
            save_progress(auctions_data)

        completed_auctions = 0
        for auction in auctions_data:
            success = await scrape_auction_details(crawler, auction)
            if success:
                completed_auctions += 1
            save_progress(auctions_data)

        logging.info(f"Scraping complete. Processed {completed_auctions} out of {len(auctions_data)} auctions. Final data saved in {output_file}")

if __name__ == "__main__":
    asyncio.run(main())