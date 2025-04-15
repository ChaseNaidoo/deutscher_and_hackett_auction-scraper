import asyncio
import json
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
import logging
import os
import re
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_existing_data(filename="menzies_auctions.json"):
    """Loads existing auction data from a JSON file, if it exists."""
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                logging.info(f"Loaded existing data with {len(data)} auctions from {filename}")
                return data
        else:
            logging.info(f"No existing data found at {filename}, starting fresh")
            return []
    except Exception as e:
        logging.error(f"Error loading existing data from {filename}: {str(e)}")
        return []

async def scrape_past_auctions(crawler, url, existing_urls=None):
    """Scrapes all auctions from the past auctions page within 2015-2025."""
    try:
        existing_urls = existing_urls or set()
        result = await crawler.arun(url=url)
        if not result.success:
            logging.error(f"Failed to crawl {url}")
            return []

        soup = BeautifulSoup(result.html, "html.parser")
        auctions_data = []
        valid_years = {str(year) for year in range(2015, 2026)}  # 2015 to 2025

        # Find catalogue entries
        catalogue_divs = soup.select("div.pageCatalogue")
        logging.info(f"Found {len(catalogue_divs)} catalogue divs")

        if not catalogue_divs:
            logging.error("No catalogue divs found")
            return []

        for catalogue in catalogue_divs:
            # Extract year from pageCatDesc
            date_elem = catalogue.find("p", class_="pageCatDesc")
            year = None
            date_text = ""
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                year_match = re.search(r"\b(202[0-5]|201[5-9])\b", date_text)
                year = year_match.group(1) if year_match else None
                logging.info(f"Date text: {date_text}, Extracted year: {year}")

            # Fallback: Check for year in anchor or headers
            if not year:
                year_anchor = catalogue.find_previous("a", id=re.compile(r"year-\d+"))
                if year_anchor:
                    year = year_anchor["id"].replace("year-", "")
                else:
                    for elem in [catalogue.find_previous("h2"), catalogue.find_previous("h3"), catalogue]:
                        if elem:
                            text = elem.get_text(strip=True)
                            year_match = re.search(r"\b(202[0-5]|201[5-9])\b", text)
                            if year_match:
                                year = year_match.group(1)
                                break
                logging.info(f"Fallback year: {year}")

            # Skip if year is not in valid range
            if year not in valid_years:
                logging.info(f"Skipping catalogue with year {year} (not in 2015-2025)")
                continue

            title_elem = catalogue.find("h3", class_="pageCatTitle") or catalogue.find("h4") or catalogue.find("h2")
            title = title_elem.get_text(strip=True) if title_elem else "No title found"

            date = date_text if date_text else "No date found"

            sale_total_elem = catalogue.find("li", string=re.compile("Sale Total:", re.I)) or \
                             catalogue.find("p", string=re.compile("Sale Total:", re.I))
            sale_total = sale_total_elem.get_text(strip=True).replace("Sale Total: ", "") if sale_total_elem else "No sale total found"

            auction_link = catalogue.find("a", class_="buTTon", href=True) or \
                          catalogue.find("a", href=re.compile(r"/catalogue-details/"))
            auction_url = auction_link["href"] if auction_link else ""
            if auction_url.startswith("/"):
                auction_url = urljoin("https://www.menziesartbrands.com", auction_url)

            if not auction_url:
                logging.warning(f"No auction URL found for catalogue: {title}")
                continue

            # Skip if auction already processed
            if auction_url in existing_urls:
                logging.info(f"Skipping already processed auction: {auction_url}")
                continue

            auctions_data.append({
                "url": auction_url,
                "title": title,
                "year": year,
                "date": date,
                "sale_total": sale_total,
                "lots": []
            })
            logging.info(f"Collected auction: {title} (Year: {year}, URL: {auction_url})")

        # Sort auctions by year (descending) for consistent processing
        auctions_data.sort(key=lambda x: x["year"], reverse=True)
        return auctions_data

    except Exception as e:
        logging.error(f"Error scraping {url}: {str(e)}")
        return []

async def scrape_lot_details(crawler, lot_url):
    """Scrapes artist, title, medium, size, and price sold from an individual lot page."""
    try:
        if lot_url.startswith("/"):
            lot_url = urljoin("https://www.menziesartbrands.com", lot_url)

        await asyncio.sleep(1)  # Delay to avoid rate-limiting
        result = await crawler.arun(url=lot_url)
        if not result.success:
            logging.error(f"Failed to crawl lot page {lot_url}")
            return None

        soup = BeautifulSoup(result.html, "html.parser")
        description_div = soup.find("div", class_="pageLotDescriptionTxt")
        details_div = soup.find("div", class_="pageLotDetails")

        # Extract artist
        artist = "Unknown Artist"
        if description_div:
            artist_p = description_div.find("p")
            artist = artist_p.get_text(strip=True) if artist_p else artist
        else:
            logging.warning(f"No description div found on lot page {lot_url}")

        # Extract title
        title = "Unknown Title"
        if description_div and artist_p:
            title_p = artist_p.find_next("p")
            if title_p:
                title_elem = title_p.find("i")
                title = title_elem.get_text(strip=True) if title_elem else title

        # Extract medium and size
        medium = "Unknown Medium"
        size = "Not specified"
        if description_div and title_p:
            medium_size_p = title_p.find_next("p")
            if medium_size_p:
                lines = [line.strip() for line in medium_size_p.get_text().split("\n") if line.strip()]
                if lines:
                    medium = lines[0]
                if len(lines) > 1:
                    size = lines[1]

        # Extract sold price
        sold_price = None
        # Try Sold For in pageLotDetails
        if details_div:
            for p in details_div.find_all("p"):
                if p.find("strong", string=re.compile(r"Sold For:", re.I)):
                    price_spans = p.find_all("span", class_="price")
                    if price_spans:
                        price_text = price_spans[0].get_text(strip=True).replace(",", "")
                        try:
                            price_num = int(price_text)
                            sold_price = f"${price_num:,}"
                            logging.info(f"Found Sold For price: {sold_price} on {lot_url}")
                        except ValueError:
                            logging.warning(f"Invalid price format in Sold For span: {price_text} on {lot_url}")
                        break
                    else:
                        logging.warning(f"No price span found in Sold For p tag on {lot_url}")
            else:
                logging.debug(f"No Sold For p tag found in pageLotDetails on {lot_url}")
        else:
            logging.warning(f"No pageLotDetails div found on {lot_url}")

        # Try Result Hammer in pageLotDescriptionTxt
        if sold_price is None and description_div:
            for p in description_div.find_all("p"):
                if p.find("strong", string=re.compile(r"Result Hammer:", re.I)):
                    price_span = p.find("span", class_="price")
                    if price_span:
                        price_text = price_span.get_text(strip=True).replace(",", "")
                        try:
                            price_num = int(price_text)
                            sold_price = f"${price_num:,}"
                            logging.info(f"Found Result Hammer price: {sold_price} on {lot_url}")
                        except ValueError:
                            logging.warning(f"Invalid price format in Result Hammer span: {price_text} on {lot_url}")
                        break
                    else:
                        logging.warning(f"No price span found in Result Hammer p tag on {lot_url}")
            else:
                logging.debug(f"No Result Hammer p tag found in pageLotDescriptionTxt on {lot_url}")

        # If no sold price found, skip the lot
        if sold_price is None:
            logging.info(f"No sold price found, skipping lot {lot_url}")
            return None

        logging.info(f"Scraped lot: {artist} - {title} (Price: {sold_price}) on {lot_url}")
        return {
            "artist": artist,
            "title": title,
            "medium": medium,
            "size": size,
            "price": sold_price,
            "url": lot_url
        }

    except Exception as e:
        logging.error(f"Error scraping lot details for {lot_url}: {str(e)}")
        return None

async def scrape_auction_details(crawler, auction):
    """Scrapes lot details from an auction page, handling pagination."""
    try:
        if auction["lots"]:
            logging.info(f"Skipping already processed auction: {auction['url']}")
            return True

        base_url = auction["url"]
        page = 1
        all_lots = []

        while True:
            url = f"{base_url}?page={page}" if page > 1 else base_url
            result = await crawler.arun(url=url)
            if not result.success:
                logging.error(f"Failed to crawl auction page {url}")
                break

            soup = BeautifulSoup(result.html, "html.parser")
            lot_rows = soup.select("div.pageListing")
            if not lot_rows:
                logging.info(f"No more lots found on page {page} for auction: {base_url}")
                break

            logging.info(f"Found {len(lot_rows)} lots on page {page} for auction: {base_url}")

            tasks = []
            for row in lot_rows:
                # Look for lot link
                lot_link = row.find("a", class_="buTTon", href=re.compile(r"/items/\d+"))
                if lot_link:
                    lot_url = urljoin("https://www.menziesartbrands.com", lot_link["href"])
                    logging.info(f"Found lot link: {lot_url}")
                    tasks.append(scrape_lot_details(crawler, lot_url))
                else:
                    logging.warning(f"No lot link found in row for auction: {base_url}")

                # Log price presence for context
                sold_price_elem = row.find("p", string=re.compile("(Sold For|Result Hammer):", re.I))
                if sold_price_elem:
                    sold_price = sold_price_elem.get_text(strip=True)
                    logging.info(f"Price found in row: {sold_price} for {base_url}")
                else:
                    logging.info(f"No price found in row for {base_url}")

            lot_details = await asyncio.gather(*tasks, return_exceptions=True)
            valid_lots = [lot for lot in lot_details if lot and not isinstance(lot, Exception)]
            all_lots.extend([
                {**lot, "auctionUrl": base_url}
                for lot in valid_lots
            ])

            logging.info(f"Scraped {len(valid_lots)} valid lots from page {page} for auction: {base_url}")

            # Check for next page
            next_page = soup.select_one("div.pageListingNav a[href*='page={}']".format(page + 1))
            if not next_page:
                logging.info(f"No next page found after page {page} for auction: {base_url}")
                break
            page += 1
            await asyncio.sleep(1)  # Avoid overwhelming the server

        auction["lots"] = all_lots
        logging.info(f"Total scraped {len(auction['lots'])} lots for auction: {base_url}")
        return True

    except Exception as e:
        logging.error(f"Error scraping auction details for {base_url}: {str(e)}")
        return False

def save_progress(auctions_data, filename="menzies_auctions.json"):
    """Saves the current state of auctions_data to a JSON file."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(auctions_data, f, indent=4)
        logging.info(f"Progress saved to {filename}")
    except Exception as e:
        logging.error(f"Error saving progress to {filename}: {str(e)}")

async def main():
    base_url = "https://www.menziesartbrands.com/auction/results"
    output_file = "menzies_auctions.json"

    # Load existing data to resume
    existing_data = load_existing_data(output_file)
    existing_urls = {auction["url"] for auction in existing_data}

    async with AsyncWebCrawler() as crawler:
        # Step 1: Collect all auctions not yet processed
        new_auctions = await scrape_past_auctions(crawler, base_url, existing_urls)
        logging.info(f"Collected {len(new_auctions)} new auctions")

        # Combine existing and new auctions
        auctions_data = existing_data + new_auctions
        auctions_data.sort(key=lambda x: x["year"], reverse=True)  # Sort by year descending

        # Step 2: Scrape details for each new auction
        for auction in new_auctions:
            logging.info(f"Processing auction: {auction['title']} ({auction['year']}) - {auction['url']}")
            success = await scrape_auction_details(crawler, auction)
            if success:
                logging.info(f"Successfully processed auction: {auction['url']}")
            else:
                logging.warning(f"Failed to process auction: {auction['url']}")
            # Save progress after each auction
            save_progress(auctions_data, output_file)

        logging.info(f"Scraping complete. Final data saved in {output_file}")

if __name__ == "__main__":
    asyncio.run(main())