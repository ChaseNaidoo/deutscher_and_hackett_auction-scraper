import json
import csv
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def json_to_csv(json_filename="auctions_with_lots.json", csv_filename="auctions_with_lots.csv"):
    """Converts the JSON data from auctions to a CSV file."""
    try:
        # Check if JSON file exists
        if not os.path.exists(json_filename):
            logging.error(f"JSON file {json_filename} not found.")
            return

        # Load JSON data
        with open(json_filename, "r", encoding="utf-8") as f:
            auctions_data = json.load(f)
        logging.info(f"Loaded JSON data with {len(auctions_data)} auctions from {json_filename}")

        # Define CSV headers (auction-level + lot-level fields)
        auction_fields = ["url", "title", "year", "location", "date", "sale_number"]
        lot_fields = ["artist", "title", "medium", "size", "signage", "provenance", "condition", "price", "url", "auctionUrl"]
        headers = auction_fields + lot_fields

        # Open CSV file for writing
        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            # Process each auction and its lots
            total_lots = 0
            for auction in auctions_data:
                auction_base = {field: auction.get(field, "") for field in auction_fields}
                for lot in auction.get("lots", []):
                    lot_data = {field: lot.get(field, "") for field in lot_fields}
                    # Combine auction-level and lot-level data
                    row = {**auction_base, **lot_data}
                    writer.writerow(row)
                    total_lots += 1

        logging.info(f"Converted JSON to CSV successfully. Saved {total_lots} lots from {len(auctions_data)} auctions to {csv_filename}")

    except Exception as e:
        logging.error(f"Error converting JSON to CSV: {str(e)}")

if __name__ == "__main__":
    json_to_csv()  # Uses default filenames: auctions_with_lots.json -> auctions_with_lots.csv