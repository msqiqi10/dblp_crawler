import requests
import pandas as pd
from datetime import datetime
import time
import os
import re
from pathlib import Path
import argparse
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    filename='./log/dblp_crawler.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def create_session_with_retries(total_retries=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504)):
    """Create a requests Session with retry strategy."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def sanitize_filename(filename):
    # Remove or replace characters that are invalid in filenames
    return re.sub(r'[\\/*?:"<>|]', "_", filename)

def download_bibtex(url, title, bib_folder, session, headers):
    try:
        response = session.get(url, headers, timeout=10)
        response.raise_for_status()
        safe_title = sanitize_filename(title)
        filename = bib_folder / f"{safe_title}.bib"
        with filename.open("w", encoding="utf-8") as file:
            file.write(response.text)
        logging.info(f"Downloaded and saved as {filename}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading BibTeX for {title}: {e}")

def get_dblp_results(keyword, venue, year, session, headers, sleep_time=5):
    """Fetch results from DBLP API for a given keyword, venues, and years."""
    base_url = "https://dblp.org/search/publ/api"  # Use HTTPS
    results = []
    
    retries = 0
    max_retries = 5
    backoff = 5  # Initial backoff time in seconds
    
    while retries <= max_retries:
        try:
            query_parts = [keyword]
            if venue.lower() != 'all':
                query_parts.append(f"streamid:conf/{venue.lower()}:")
            if str(year).lower() != 'all':
                query_parts.append(f"year:{year}:")
            query_string = ' '.join(query_parts)
            
            
            params = {
                "q": query_string,
                "format": "json",
                "h": 100,  # Max number of results to fetch (max: 1000)
            }
            response = session.get(base_url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff))
                logging.warning(f"Received 429 for '{keyword}' in {venue} {year}. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
                retries += 1
                backoff *= 2  # Exponential backoff
                continue  # Retry the request
            
            response.raise_for_status()
            data = response.json()
            
            total_hits = data.get("result", {}).get("hits", {}).get("@total", "0")
            if total_hits == "0":
                logging.info(f"No results for '{keyword}' in {venue} {year}.")
                break  # No need to retry
            
            count = int(total_hits)
            hits = data["result"]["hits"].get("hit", [])
            
            # Ensure hits is a list
            if isinstance(hits, dict):
                hits = [hits]
            
            for hit in hits:
                info = hit.get("info", {})
                authors_info = info.get("authors", {}).get("author", [])
                if isinstance(authors_info, list):
                    authors = ", ".join(author.get("text", "") for author in authors_info)
                elif isinstance(authors_info, dict):
                    authors = authors_info.get("text", "")
                else:
                    authors = ""
            
                entry = {
                    "title": info.get("title", ""),
                    "authors": authors,
                    "venue": info.get("venue", ""),
                    "year": info.get("year", ""),
                    "url": info.get("url", ""),
                }
                results.append(entry)
            
            logging.info(f"'{keyword}' {venue} {year}: {count} results")
            time.sleep(sleep_time)  # Adjust based on API rate limits
            break  # Successful request, move to next
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for '{keyword}' {venue} {year}: {e}")
            retries += 1
            if retries > max_retries:
                logging.error(f"Max retries exceeded for '{keyword}' {venue} {year}'. Skipping...")
                break
            logging.info(f"Retrying ({retries}/{max_retries}) after {backoff} seconds...")
            time.sleep(backoff)
            backoff *= 2  # Exponential backoff
        except KeyError as e:
            logging.error(f"Unexpected data format for '{keyword}' {venue} {year}: Missing key {e}")
            break  # Skip to next iteration
    
    return results


def save_results_to_excel_file(filename, all_results):
    """
    Saves all_results to an Excel file with each keyword's results in a separate sheet.
    
    :param filename: The name of the Excel file.
    :param all_results: A dictionary where keys are sheet names and values are lists of entries.
    """
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, results in all_results.items():
                if not results:
                    logging.info(f"No results to save for keyword: {sheet_name}")
                    continue
                df = pd.DataFrame(results)
                # Excel sheet names have a maximum length of 31 characters
                safe_sheet_name = sanitize_filename(sheet_name)[:31]
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        logging.info(f"All results have been saved to {filename}.")
    except Exception as e:
        logging.error(f"Error saving results to Excel file: {e}")

def parse_args():
    parser = argparse.ArgumentParser(
        description='Description of your script goes here',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # Shows defaults in help message
    )
    
    parser.add_argument('--save_bibtex', action='store_true')
    parser.add_argument('-k', '--keywords', nargs='+', default=['data condensation', 'data distillation'])
    parser.add_argument('-v', '--venues', nargs='+', default=["ICLR", "ICML", "NIPS", "AAAI", "KDD", "ICDM", "WSDM", "WWW", "CIKM", "IJCAI", "CVPR", "ICCV", "ECCV"])
    parser.add_argument('-y', '--years', nargs='+', default=['2024', '2023'])
    parser.add_argument('-o', '--outdir', default='./data_condensation')
    
    return parser.parse_args()

def main():
    args = parse_args()

    all_results = {}
    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True)
    excel_filename = Path(os.path.join(args.outdir, 'results.xlsx'))
    # Create a session with retries
    session = create_session_with_retries()
    
    # Define headers with a custom User-Agent
    headers = {
        "User-Agent": "DBLPCrawler/1.0 (contact@example.com)"  # Replace with your actual contact
    }
    if args.save_bibtex:
        bib_dir = outdir / 'bibs'
        bib_dir.mkdir(parents=True, exist_ok=True)
   
    for keyword in args.keywords:
        keyword_results = []
        for year in args.years:
            for venue in args.venues:
                logging.info(f"Fetching results for keyword: {keyword}, {year}, {venue}")
                results = get_dblp_results(keyword, venue, year, session, headers)
                keyword_results.extend(results)
                if args.save_bibtex:
                    for r in results:
                        download_bibtex(r['url'], r['title'], bib_dir, session, headers)
        all_results[keyword] = keyword_results

    # Specify the Excel filename
    save_results_to_excel_file(excel_filename, all_results)
    
    session.close()

if __name__ == "__main__":
    main()