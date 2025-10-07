import requests
import pandas as pd
import time
import logging
from pathlib import Path

# --- Directories ---
Path("logs").mkdir(exist_ok=True)  # ensure a logs folder exists
Path("data").mkdir(exist_ok=True)


# --- Config ---

BASE_URL = "https://api.worldbank.org/v2/sources/14/country/{}/series/all"

country_codes = ["EGY", "MAR", "SAU", "JOR", "TUN", "IRQ", "YEM", "OMN", "QAT", "BHR", "KWT", "DZA", "LBY"]
params = {"format": "json", "per_page": 1000}


# ============================================================
# Helper functions
# ============================================================

def fetch_one_page(url:str, params:dict, max_retries: int =3, delay: float = 2.0) -> dict:
    
    """
    Fetch one page of results with retry logic and polite delay.
    """
    
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status() #fail if not HTTP error
            return r.json() #top level is a dict for this API
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1} failed for {url} (page {params.get('page')}).Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1)) #exponential backoff
            else:
                logging.error(f"❌ Giving up on {url} page {params.get('page')}.Error: {e}")
                return{}
    

def to_lookup(var_list):
    
    """
    Turn a list of concept dictionaries into a lookup by concept name.
    Example:
        [{'concept':'Country',...}, {'concept':'Series',...}]
    becomes
        {'Country': {...}, 'Series': {...}}
    """
    
    lookup = {}
    
    for item in var_list or []:
        if isinstance(item,dict) and "concept" in item:
            concept_name = item["concept"]
            lookup[concept_name] = item
    return lookup


def parse_rows(row: dict) -> dict:
    
    """Flatten one raw JSON row into a tidy dictionary."""

    var_list = row.get("variable", [])
    lk = to_lookup(var_list)
    
    country_id = lk.get("Country", {}).get("id")
    country_name = lk.get("Country",{}).get("value")
     
    series_id = lk.get("Series",{}).get("id")
    series_name = lk.get("Series",{}).get("value")
    
    
    time_id = lk.get("Time",{}).get("id")
    year = None
    
    if isinstance(time_id,str) and time_id.startswith("YR"):
        try:
            year = int(time_id[2:]) # remove "YR" and convert to integer
        except ValueError:
            year = None
        
    value = row.get("value")
    
    # Return one flat dictionary
    
    return{
        "country_id": country_id,
        "country_name": country_name,
        "series_id": series_id,
        "series_name": series_name,
        "year": year,
        "value": value
    }
    
# ============================================================
# Main extraction loop
# ============================================================

all_country_dfs = []

for code in country_codes:
    # --------------------------------------------------------
    # Set up a logger unique to this country
    # --------------------------------------------------------
    
    logger = logging.getLogger(code)
    logger.setLevel(logging.INFO)
    
    # file + console handlers
    file_handler = logging.FileHandler(f"logs/{code}_fetch.log", mode="w")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    
    # attach handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    
    # --------------------------------------------------------
    # Begin fetching
    # --------------------------------------------------------
    
    
    logger.info(f"Fetching All pages for {code} ...")
    params = {"format": "json", "per_page": 1000, "page": 1}
    url = BASE_URL.format(code)
    
    #NEW: hold this country's DFs here
    dfs_this_country = [] 
    
    payload = fetch_one_page(url, params)
    if not payload:
        logger.warning(f"⚠️ SKIPPING : No payload for {code} page 1.")
        continue
    
    #NEW : how many pages exist for this country?
    total_pages = payload.get("pages",1)
    
    rows = payload.get("source", {}).get("data", [])
    parsed = [parse_rows(r) for r in rows if isinstance(r,dict)]
    df_page1 = pd.DataFrame(parsed)
    if not df_page1.empty:
        df_page1['value'] = pd.to_numeric(df_page1['value'], errors='coerce')
        dfs_this_country.append(df_page1) #Changed here to append to new list
    else:
        logger.warning(f"⚠️ SKIPPING Country : {code} page 1 is empty.")
        continue
    
    # --------------------------------------------------------
    # Remaining pages
    # --------------------------------------------------------
    
    for p in range(2, total_pages +1):
        params["page"] = p
        payload_p = fetch_one_page(url, params)
        
        if not payload_p:
            logger.warning(f"⚠️ Skipping page {p} for {code} (no response).")
            continue
        
        rows_p = payload.get("source", {}).get("data", [])
        parsed_p = [parse_rows(r) for r in rows_p if isinstance(r,dict)]
        df_p = pd.DataFrame(parsed_p)
        
        # 🟧 Stop early if page empty
        if df_p.empty:
            logger.info(f"ℹ️ Page {p} for {code} is empty — stopping early.")
            break
        
        df_p["value"] = pd.to_numeric(df_p["value"], errors="coerce")
        dfs_this_country.append(df_p)
        
        time.sleep(0.2) #polite delay
    
    
    # --------------------------------------------------------
    # Finalize this country
    # --------------------------------------------------------
    
    #NEW: finalize one country
    if dfs_this_country: #not empty
        df_country = pd.concat(dfs_this_country, ignore_index=True)
        df_country['requested_country'] = code
        all_country_dfs.append(df_country)
        
        df_country.to_csv(f"data/{code}_raw.csv", index=False)
        logger.info(f"✅ Finished {code} with {len(df_country)} rows. Saved to data/{code}_raw.csv.")
    else:
        logger.warning(f"⚠️ No data collected for {code}.")


    # detach handlers (important to avoid duplicate logs)
    logger.removeHandler(file_handler)
    logger.removeHandler(stream_handler)
    file_handler.close()
    stream_handler.close()

# ============================================================
# Combine all results
# ============================================================
if all_country_dfs:
    df = pd.concat(all_country_dfs, ignore_index=True)
    df.to_csv("data/all_countries_combined.csv", index=False)
    print(f"\n✅ Combined dataset saved → data/all_countries_combined.csv ({len(df)} rows)")
else:
    print("\n⚠️ No data fetched for any country.")
    

print("\nFinal Summary:")
logger.info("Total rows:", len(df))
if not df.empty:
    logger.info("Unique countries:", df['country_id'].nunique())
    logger.info("Unique series:", df['series_id'].nunique())


    

    
