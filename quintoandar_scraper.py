import requests
import json
import csv
import sys
import os

def load_existing_ids(filepath):
    """
    Loads all IDs from the 'id' column of an existing CSV file into a set.
    """
    if not os.path.exists(filepath):
        return set()
    
    existing_ids = set()
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'id' in row:
                    existing_ids.add(row['id'])
    except Exception as e:
        print(f"Warning: Could not read existing file {filepath}. Will start fresh. Error: {e}")
        return set()
        
    return existing_ids

def fetch_quintoandar_data(city_slug, total_size):
    """
    Fetches a specified number of property listings in a single API request.
    """
    url = "https://apigw.prod.quintoandar.com.br/house-listing-search/v2/search/list"
    headers = {'Content-Type': 'application/json'}

    fields_to_request = [
        "id", "coverImage", "rent", "totalCost", "salePrice",
        "iptuPlusCondominium", "area", "imageList", "imageCaptionList",
        "address", "regionName", "city", "visitStatus",
        "activeSpecialConditions", "type", "forRent", "forSale",
        "isPrimaryMarket", "bedrooms", "parkingSpaces", "suites",
        "listingTags", "yield", "yieldStrategy", "neighbourhood",
        "categories", "bathrooms", "isFurnished", "installations",
        "amenities", "shortRentDescription", "shortSaleDescription"
    ]

    payload = {
        "filters": {"businessContext": "RENT", "location": {"countryCode": "BR"}},
        "pagination": {"pageSize": total_size, "offset": 0},
        "slug": city_slug,
        "fields": fields_to_request,
        "locationDescriptions": [{"description": city_slug}]
    }

    try:
        print(f"Making a single request to fetch up to {total_size} listings...")
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()
        
        if data.get("hits", {}).get("hits"):
            listings = [hit["_source"] for hit in data["hits"]["hits"]]
            return listings, fields_to_request
        else:
            return [], fields_to_request

    except requests.exceptions.RequestException as e:
        print(f"\nAn error occurred with the request for {city_slug}: {e}")
        return None, None
    except json.JSONDecodeError:
        print(f"\nFailed to decode the response as JSON for {city_slug}.")
        return None, None

def save_to_csv(data, headers, filepath):
    """
    Appends new data to a CSV file. Creates the file and writes headers if it doesn't exist.
    """
    if not data or not headers:
        print("No new data to save.")
        return

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file_exists = os.path.exists(filepath)
    
    with open(filepath, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction='ignore')
        
        if not file_exists:
            writer.writeheader()
            
        writer.writerows(data)
    
    print(f"Successfully saved/appended {len(data)} new listings to {filepath}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python quintoandar_scraper.py <page_multiplier> <city_slug_1> <city_slug_2> ...")
        print("\nExample: python quintoandar_scraper.py 5 sao-paulo-sp-brasil rio-de-janeiro-rj-brasil")
        print("This will request 500 listings for São Paulo and 500 for Rio de Janeiro.")
        sys.exit(1)

    try:
        page_multiplier = int(sys.argv[1])
        if page_multiplier < 1:
            print("Error: The page_multiplier must be at least 1.")
            sys.exit(1)
    except ValueError:
        print("Error: The first argument (page_multiplier) must be an integer.")
        sys.exit(1)

    city_slugs = sys.argv[2:]
    output_dir = "results/quintoandar"
    BASE_PAGE_SIZE = 1
    total_to_fetch = page_multiplier * BASE_PAGE_SIZE

    print(f"Starting scraper for {len(city_slugs)} cities.")
    print(f"Requesting up to {total_to_fetch} listings per city.")
    
    for city_slug in city_slugs:
        print(f"\n{'='*20} Processing city: {city_slug} {'='*20}")
        
        filename = f"quintoandar_{city_slug.replace('-', '_')}.csv"
        output_filepath = os.path.join(output_dir, filename)
        
        os.makedirs(output_dir, exist_ok=True)
        
        existing_ids = load_existing_ids(output_filepath)
        print(f"Found {len(existing_ids)} existing listings in {output_filepath}")

        listings, fields = fetch_quintoandar_data(city_slug, total_to_fetch)

        if listings:
            unique_new_listings = {str(item['id']): item for item in listings}.values()
            final_listings_to_add = [
                listing for listing in unique_new_listings 
                if str(listing['id']) not in existing_ids
            ]
            
            print(f"Fetched {len(listings)} listings from the API.")
            print(f"Found {len(final_listings_to_add)} new, unique listings to add for {city_slug}.")
            
            save_to_csv(final_listings_to_add, fields, output_filepath)
        else:
            print(f"No new listings found or an error occurred for {city_slug}.")

    print(f"\n{'='*20} All cities processed. {'='*20}")