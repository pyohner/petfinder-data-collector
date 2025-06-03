import requests
import json
import time
import os
from datetime import datetime
from requests.exceptions import HTTPError
from dotenv import load_dotenv

# --- LOAD API CREDENTIALS FROM .env ---
load_dotenv()  # Loads from .env file if present

API_KEY = os.getenv("PETFINDER_API_KEY")
API_SECRET = os.getenv("PETFINDER_API_SECRET")

# --- CONFIG ---
LOCATION = "orlando, fl"
LIMIT = 100
MAX_PAGES = 100
OUTPUT_DIR = "data_snapshots"
TOKEN_CACHE_FILE = "token_cache.json"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- TOKEN CACHING ---
def get_auth_token():
    if os.path.exists(TOKEN_CACHE_FILE):
        with open(TOKEN_CACHE_FILE, "r") as f:
            cached = json.load(f)
            if time.time() < cached["expires_at"]:
                print("🔁 Using cached access token.")
                return cached["access_token"]

    print("🔐 Requesting new access token...")
    url = "https://api.petfinder.com/v2/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": API_KEY,
        "client_secret": API_SECRET
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    token_data = response.json()
    token_data["expires_at"] = time.time() + token_data["expires_in"] - 10

    with open(TOKEN_CACHE_FILE, "w") as f:
        json.dump(token_data, f)

    return token_data["access_token"]


# --- SAFE GET WITH RETRIES ---
def safe_get(url, headers, params, retries=3):
    for attempt in range(retries):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            wait_time = int(response.headers.get("Retry-After", 10))
            print(f"⏳ Rate limit hit. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            continue
        try:
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            print(f"❌ HTTP Error: {e}")
            break
    return None


# --- FETCH ANIMALS ---
def fetch_animals(token, limit=LIMIT, pages=MAX_PAGES):
    headers = {"Authorization": f"Bearer {token}"}
    all_animals = []

    for page in range(1, pages + 1):
        print(f"📦 Fetching animal page {page}/{pages}...")
        params = {
            "limit": limit,
            "page": page,
            "location": LOCATION,
            "sort": "recent"
        }
        url = "https://api.petfinder.com/v2/animals"
        data = safe_get(url, headers, params)
        if not data:
            break

        animals = data.get("animals", [])
        if not animals:
            print(f"⛔ No animals returned on page {page}. Stopping early.")
            break

        all_animals.extend(animals)

        if len(animals) < limit:
            print(f"✅ Fewer than {limit} results on page {page}. Assuming end of data.")
            break

        time.sleep(0.2)

    print(f"✅ Finished fetching animals. Total: {len(all_animals)}")
    return all_animals


# --- CLEAN ANIMAL DATA ---
def clean_animal_data(animals):
    cleaned = []
    for a in animals:
        breeds = a.get("breeds") or {}
        attributes = a.get("attributes") or {}
        environment = a.get("environment") or {}
        photos = a.get("photos") or []
        primary_photo = (a.get("primary_photo_cropped") or {}).get("medium")

        cleaned.append({
            "id": a.get("id"),
            "name": a.get("name"),
            "type": a.get("type"),
            "species": a.get("species"),
            "breeds": {
                "primary": breeds.get("primary"),
                "secondary": breeds.get("secondary"),
                "mixed": breeds.get("mixed")
            },
            "age": a.get("age"),
            "gender": a.get("gender"),
            "size": a.get("size"),
            "coat": a.get("coat"),
            "attributes": {
                "spayed_neutered": attributes.get("spayed_neutered"),
                "house_trained": attributes.get("house_trained"),
                "special_needs": attributes.get("special_needs"),
                "shots_current": attributes.get("shots_current")
            },
            "environment": {
                "children": environment.get("children"),
                "dogs": environment.get("dogs"),
                "cats": environment.get("cats")
            },
            "tags": a.get("tags") or [],
            "description": a.get("description"),
            "photos": [p.get("medium") for p in photos if isinstance(p, dict) and p.get("medium")],
            "primary_photo": primary_photo,
            "status": a.get("status"),
            "published_at": a.get("published_at"),
            "status_changed_at": a.get("status_changed_at"),
            "organization_id": a.get("organization_id"),
            "url": a.get("url")
        })
    return cleaned


# --- FETCH ORGANIZATIONS ---
def fetch_organizations_by_location(token, location=LOCATION, limit=100, pages=10):
    print(f"📍 Fetching organizations in {location}...")
    headers = {"Authorization": f"Bearer {token}"}
    all_orgs = []

    for page in range(1, pages + 1):
        print(f"🏢 Fetching organization page {page}/{pages}...")
        params = {
            "location": location,
            "limit": limit,
            "page": page
        }
        url = "https://api.petfinder.com/v2/organizations"
        data = safe_get(url, headers, params)
        if not data:
            break

        orgs = data.get("organizations", [])
        if not orgs:
            print(f"⛔ No organizations returned on page {page}. Stopping early.")
            break

        all_orgs.extend(orgs)

        if len(orgs) < limit:
            print(f"✅ Fewer than {limit} results on page {page}. Assuming end of data.")
            break

        time.sleep(0.2)

    print(f"✅ Finished fetching organizations. Total: {len(all_orgs)}")
    return all_orgs


# --- CLEAN ORGANIZATION DATA ---
def clean_organization_data(orgs):
    cleaned = []
    for o in orgs:
        address = o.get("address") or {}
        adoption = o.get("adoption") or {}
        social = o.get("social_media") or {}
        photos = o.get("photos") or []

        cleaned.append({
            "id": o.get("id"),
            "name": o.get("name"),
            "email": o.get("email"),
            "phone": o.get("phone"),
            "address": {
                "city": address.get("city"),
                "state": address.get("state"),
                "postcode": address.get("postcode")
            },
            "url": o.get("url"),
            "website": o.get("website"),
            "mission_statement": o.get("mission_statement"),
            "adoption": {
                "policy": adoption.get("policy"),
                "url": adoption.get("url")
            },
            "social_media": {
                "facebook": social.get("facebook"),
                "instagram": social.get("instagram")
            },
            "photo": (photos[0].get("medium") if photos and isinstance(photos[0], dict) else None)
        })
    return cleaned


# --- MATCH ORGS TO ANIMALS ---
def match_organizations(animals, orgs):
    org_lookup = {org["id"]: org for org in orgs}
    for a in animals:
        org_id = a.get("organization_id")
        if org_id and org_id in org_lookup:
            org = org_lookup[org_id]
            a["organization"] = {
                "name": org.get("name"),
                "email": org.get("email"),
                "phone": org.get("phone"),
                "address": org.get("address"),
                "website": org.get("website"),
                "url": org.get("url"),
                "photo": org.get("photo")
            }
    return animals


# --- SAVE TO FILE ---
def save_to_file(data, name_prefix):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = os.path.join(OUTPUT_DIR, f"{name_prefix}_{date_str}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved {name_prefix} to {filename}")


# --- MAIN ---
def main():
    token = get_auth_token()

    animals_raw = fetch_animals(token)
    animals_clean = clean_animal_data(animals_raw)
    save_to_file(animals_clean, "data")

    orgs_raw = fetch_organizations_by_location(token)
    orgs_clean = clean_organization_data(orgs_raw)
    save_to_file(orgs_clean, "organizations")

    animals_with_orgs = match_organizations(animals_clean, orgs_clean)
    save_to_file(animals_with_orgs, "data_with_orgs")


if __name__ == "__main__":
    main()
