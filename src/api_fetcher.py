import requests
import pandas as pd
import time



BASE_URL = "https://api.worldbank.org/v2/sources/14/country/{}/series/all"

country_codes = ["EGY", "MAR", "SAU", "JOR", "TUN", "IRQ", "YEM", "OMN", "QAT", "BHR", "KWT", "DZA", "LBY"]
params = {"format": "json", "per_page": 1000}

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
            print(f"Attempt {attempt + 1} failed for {url} (page {params.get('page')}).Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1)) #exponential backoff
            else:
                print("❌ Giving up on this page.")
                return{}
    


# --- Step 1C – Convert the 'variable' list into a lookup dict ---

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

# --- Step 1D: Parse one row into a flat, tidy dictionary ---

def parse_rows(row: dict) -> dict:
    
    # 1️⃣ Use the helper from Step 1C to turn the 'variable' list into a lookup dict
    
    var_list = row.get("variable", [])
    lk = to_lookup(var_list)
    
    # 2️⃣ Extract country information
    
    country_id = lk.get("Country", {}).get("id")
    country_name = lk.get("Country",{}).get("value")
     
    # 3️⃣ Extract indicator / series information
    
    series_id = lk.get("Series",{}).get("id")
    series_name = lk.get("Series",{}).get("value")
    
    # 4️⃣ Extract and clean time information
    
    time_id = lk.get("Time",{}).get("id")
    year = None
    
    if isinstance(time_id,str) and time_id.startswith("YR"):
        try:
            year = int(time_id[2:]) # remove "YR" and convert to integer
        except ValueError:
            year = None
        
    # 5️⃣ Extract the numeric value (can be None)
    
    value = row.get("value")
    
    # 6️⃣ Return one flat dictionary
    
    return{
        "country_id": country_id,
        "country_name": country_name,
        "series_id": series_id,
        "series_name": series_name,
        "year": year,
        "value": value
    }
    

all_country_dfs = []

for code in country_codes:
    print(f"Fetching All pages for {code} ...")
    params = {"format": "json", "per_page": 1000, "page": 1}
    url = BASE_URL.format(code)

    payload = fetch_one_page(url, params)
    
    #NEW : how many pages exist for this country?
    total_pages = payload.get("pages",1)
    #NEW: hold this country's DFs here
    dfs_this_country = [] 


    rows = payload.get("source", {}).get("data", [])
    parsed = [parse_rows(r) for r in rows if isinstance(r,dict)]
    df_page1 = pd.DataFrame(parsed)
    if not df_page1.empty:
        df_page1['value'] = pd.to_numeric(df_page1['value'], errors='coerce')
        dfs_this_country.append(df_page1) #Changed here to append to new list

    for p in range(2, total_pages +1):
        params["page"] = p
        payload_p = fetch_one_page(url, params)
        
        if not payload_p:
            print(f"⚠️ Skipping page {p} for {code} (no response).")
            continue
        
        rows_p = payload.get("source", {}).get("data", [])
        parsed_p = [parse_rows(r) for r in rows_p if isinstance(r,dict)]
        df_p = pd.DataFrame(parsed_p)
        if not df_p.empty:
            
            #After you parse a page, check if it’s empty — if so, you can break early:
            
            print(f"ℹ️ Page {p} for {code} is empty — stopping early.")
            break
        
        df_p["value"] = pd.to_numeric(df_p["value"], errors="coerce")
        dfs_this_country.append(df_p)
        
        time.sleep(0.2)
    
    #NEW: finalize one country
    if dfs_this_country: #not empty
        df_country = pd.concat(dfs_this_country, ignore_index=True)
        df_country['requested_country'] = code
        all_country_dfs.append(df_country)
    else:
        print(f"No data for country {code}")



# Combine the first pages from all countries
df = pd.concat(all_country_dfs, ignore_index=True) if all_country_dfs else pd.DataFrame()
print("\nFinal Summary:")
print("Total rows:", len(df))
if not df.empty:
    print("Unique countries:", df['country_id'].nunique())
    print("Unique series:", df['series_id'].nunique())


    

    
