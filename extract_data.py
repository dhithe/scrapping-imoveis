import json
import time
import os
import argparse
from datetime import datetime
from typing import Dict, Any, List, Set
import threading
import cloudscraper
import pandas as pd
from pathlib import Path

# --- CORRECT, SIMPLE API CODES ---
# Based on your working example and further verification.
CITY_CODES = {
    "itanhaem-sp": "109237",
}

class ImovelWebScraper:
    def __init__(self, city_code: str, city_slug: str, seen_posting_ids: Set[str], base_state_file_name: str = "scraper_state", cookies_file: str = "cookies.json"):
        self.city_slug = city_slug
        self.city_code = city_code
        self.seen_posting_ids = seen_posting_ids
        self.state_file = f"{base_state_file_name}_{self.city_slug.replace('-', '_')}.json"
        self.cookies_file = cookies_file
        
        self.session = cloudscraper.create_scraper()
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
            "Accept": "*/*", "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
            "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.imovelweb.com.br", "Connection": "keep-alive",
        }
        self.session.headers.update(self.headers)

        self.url = "https://www.imovelweb.com.br/rplis-api/postings"
        self.params = {"dynamicListingSearch": "true", "enableStepSA": "true"}

        # --- CORRECTED BASE PAYLOAD ---
        # This now matches the structure of your working example.
        self.base_payload = {
            "q": None, "direccion": None, "moneda": "", "preciomin": None, "preciomax": None,
            "services": "", "general": "", "searchbykeyword": "", "amenidades": "",
            "caracteristicasprop": None, "comodidades": "", "disposicion": None,
            "roomType": "", "outside": "", "areaPrivativa": "", "areaComun": "",
            "multipleRets": "", "tipoDePropiedad": "2", "subtipoDePropiedad": None,
            "tipoDeOperacion": "2", "garages": None, "antiguedad": None,
            "expensasminimo": None, "expensasmaximo": None, "withoutguarantor": None,
            "habitacionesminimo": 0, "habitacionesmaximo": 0, "ambientesminimo": 0,
            "ambientesmaximo": 0, "banos": None, "superficieCubierta": 1,
            "idunidaddemedida": 1, "metroscuadradomin": None, "metroscuadradomax": None,
            "tipoAnunciante": "ALL", "grupoTipoDeMultimedia": "", "publicacion": None,
            "sort": "relevance", "etapaDeDesarrollo": "", "auctions": None,
            "polygonApplied": None, "idInmobiliaria": None, "excludePostingContacted": "",
            "banks": "", "places": "", "condominio": "", "preTipoDeOperacion": "",
            "pagina": 1, "city": None, "province": None, "zone": None, "valueZone": None,
            "subZone": None, "coordenates": None
        }

        self.new_listings_this_session = {}
        self.state = self._load_state()
        self._load_cookies()
        self.new_listings_count = 0

    def _load_state(self) -> Dict[str, Any]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError: return {"last_page": 0}
        return {"last_page": 0}

    def _save_state(self, page: int) -> None:
        self.state["last_page"] = page
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)

    def _load_cookies(self) -> None:
        if os.path.exists(self.cookies_file):
            try:
                with open(self.cookies_file, 'r', encoding='utf-8') as f:
                    self.session.cookies.update(json.load(f))
            except json.JSONDecodeError: print(f"Error loading cookies from {self.cookies_file}.")

    def scrape_page(self, page: int) -> bool:
        try:
            self.session.headers["Referer"] = f"https://www.imovelweb.com.br/imoveis-aluguel-{self.city_slug}-{page}.html"
            
            # --- FINAL PAYLOAD LOGIC ---
            request_payload = self.base_payload.copy()
            request_payload['city'] = self.city_code # Use the correct integer code
            request_payload['province'] = None      # CRITICAL: Ensure province is None
            request_payload['pagina'] = page        # Use the correct pagination key

            print(f"Requesting page {page} for city {self.city_slug}...")
            response = self.session.post(self.url, params=self.params, json=request_payload, timeout=45)

            if response.status_code == 200:
                data = response.json()
                listings_on_page = data.get("listPostings", [])
                if not listings_on_page:
                    print(f"Page {page} ({self.city_slug}): No more listings found.")
                    return False

                new_in_session_count = 0
                for listing_item in listings_on_page:
                    posting_id = str(listing_item.get("postingId"))
                    if posting_id and posting_id not in self.seen_posting_ids and posting_id not in self.new_listings_this_session:
                        self.new_listings_this_session[posting_id] = listing_item
                        new_in_session_count += 1
                
                self.new_listings_count += new_in_session_count
                print(f"Page {page} ({self.city_slug}): Found {len(listings_on_page)} listings, {new_in_session_count} are new.")
                self._save_state(page)
                return True
            else:
                print(f"Error: Status code {response.status_code} for page {page}. Response: {response.text[:200]}")
                return False
        except Exception as e:
            print(f"An unexpected error occurred on page {page}: {e}")
            return False

    def scrape(self, start_page: int = None, delay: float = 5.0, max_pages: int = None) -> Dict[str, Any]:
        current_page = start_page or self.state.get("last_page", 0) + 1
        print(f"Starting scraper for '{self.city_slug}' at page {current_page}. Known listings for this city: {len(self.seen_posting_ids)}")
        
        pages_scraped_this_run = 0
        try:
            while True:
                if max_pages and pages_scraped_this_run >= max_pages:
                    print(f"Reached max pages ({max_pages}) for {self.city_slug}.")
                    break
                if not self.scrape_page(current_page): break
                
                pages_scraped_this_run += 1
                current_page += 1
                time.sleep(delay)
        finally:
            print(f"Finished scraping for {self.city_slug}. Found {self.new_listings_count} new listings in this session.")
            return self.new_listings_this_session

def load_seen_ids_from_city_csvs(directory: Path, city_slug: str) -> Set[str]:
    if not directory.exists(): return set()
    seen_ids = set()
    city_csv_pattern = f"*_{city_slug}_results.csv"
    csv_files = list(directory.glob(city_csv_pattern))
    if not csv_files: return seen_ids
    for file in csv_files:
        try:
            df = pd.read_csv(file, usecols=['postingId'], dtype={'postingId': str})
            seen_ids.update(df['postingId'].dropna().tolist())
        except Exception as e:
            print(f"Warning: Could not read {file}. Error: {e}")
    return seen_ids

def save_new_listings_to_city_csv(new_listings: List[Dict[str, Any]], directory: Path, city_slug: str):
    if not new_listings:
        print(f"No new listings to save for '{city_slug}'.")
        return

    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = directory / f"{timestamp}_{city_slug}_results.csv"
    flattened_data = []
    for listing in new_listings:
        price, currency, expenses = None, None, None
        try:
            price_info = listing['priceOperationTypes'][0]['prices'][0]
            price = price_info.get('amount'); currency = price_info.get('currency')
        except (KeyError, TypeError, IndexError): pass
        try: expenses = listing['expenses']['amount']
        except (KeyError, TypeError): pass
        bedrooms, bathrooms, area, suites, parking = None, None, None, None, None
        try:
            features_dict = listing.get('mainFeatures', {})
            if isinstance(features_dict, dict):
                area_util = next((f.get('value') for f in features_dict.values() if 'útil' in f.get('label','').lower()), None)
                area_total = next((f.get('value') for f in features_dict.values() if 'total' in f.get('label','').lower()), None)
                area = area_util or area_total
                bedrooms = next((f.get('value') for f in features_dict.values() if 'quarto' in f.get('label','').lower()), None)
                bathrooms = next((f.get('value') for f in features_dict.values() if 'banheiro' in f.get('label','').lower()), None)
                suites = next((f.get('value') for f in features_dict.values() if 'suíte' in f.get('label','').lower()), None)
                parking = next((f.get('value') for f in features_dict.values() if 'vaga' in f.get('label','').lower()), None)
        except (AttributeError, TypeError): pass
        location, latitude, longitude = None, None, None
        try:
            location = listing['postingLocation']['location']['name']
            geo = listing['postingLocation']['postingGeolocation']['geolocation']
            latitude, longitude = geo.get('latitude'), geo.get('longitude')
        except (KeyError, TypeError, IndexError): pass
        full_url = None
        try:
            base_url = "https://www.imovelweb.com.br"
            relative_url = listing.get('url', '')
            if relative_url: full_url = base_url + relative_url
        except TypeError: pass
        flat_item = {'postingId': listing.get('postingId'),'price': price, 'expenses': expenses, 'currency': currency, 'bedrooms': bedrooms, 'suites': suites, 'bathrooms': bathrooms, 'parking': parking, 'area_m2': area, 'location': location, 'latitude': latitude, 'longitude': longitude, 'title': listing.get('title'), 'description': listing.get('descriptionNormalized'),'publisher': listing.get('publisher', {}).get('name'), 'full_url': full_url,'retrievedDate': timestamp}
        flattened_data.append(flat_item)

    df = pd.DataFrame(flattened_data)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Success! Saved {len(new_listings)} new listings for '{city_slug}' to: {output_path}")

def scrape_city_task(city_slug: str, city_code: str, output_dir: Path, args: argparse.Namespace):
    """A self-contained task for a single thread to scrape and save one city."""
    seen_ids = load_seen_ids_from_city_csvs(output_dir, city_slug)
    scraper = ImovelWebScraper(
        city_slug=city_slug, city_code=city_code, seen_posting_ids=seen_ids,
        base_state_file_name=args.state_base_name, cookies_file=args.cookies
    )
    new_city_listings = scraper.scrape(start_page=args.start, delay=args.delay, max_pages=args.max_pages)
    if new_city_listings:
        save_new_listings_to_city_csv(list(new_city_listings.values()), output_dir, city_slug)

def main():
    parser = argparse.ArgumentParser(description="Scrape new property listings from imovelweb.com.br.")
    parser.add_argument("--city", required=True, nargs='+', help="One or more city slugs (e.g., santos-sp sao-paulo-sp)")
    parser.add_argument("--start", type=int, help="Starting page number (overrides saved state for all cities)")
    parser.add_argument("--state-base-name", default="scraper_state", help="Base name for city-specific state files")
    parser.add_argument("--cookies", default="cookies.json", help="Shared cookies file for initial load")
    parser.add_argument("--delay", type=float, default=5.0, help="Delay between requests in seconds (per thread)")
    parser.add_argument("--max-pages", type=int, help="Maximum pages to scrape per city")
    args = parser.parse_args()
    output_directory = Path("results") / "imovelweb"
    threads = []
    for city_slug_item in args.city:
        city_code = CITY_CODES.get(city_slug_item)
        if not city_code:
            print(f"!!! WARNING: City slug '{city_slug_item}' is not defined in CITY_CODES dictionary. Skipping this city.")
            continue
        thread = threading.Thread(target=scrape_city_task, args=(city_slug_item, city_code, output_directory, args))
        threads.append(thread)
        thread.start()
        time.sleep(2)
    for thread in threads:
        thread.join()
    print("\nAll scraping threads have completed. Scraping session finished.")

if __name__ == "__main__":
    main()