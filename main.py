import requests
import json
import time
import os
from datetime import datetime
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import sqlite3
from pathlib import Path


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
today_str = datetime.today().strftime("%Y-%m-%d")
base_dir = Path(__file__).resolve().parent
animal_file = base_dir / f"data_snapshots/data_{today_str}.json"
org_file = base_dir / f"data_snapshots/organizations_{today_str}.json"
DB_FILE = Path("C:/Users/yohnep25/PycharmProjects/databases/petfinder_data.db")


# --- TOKEN CACHING ---
def get_auth_token():
    if os.path.exists(TOKEN_CACHE_FILE):
        with open(TOKEN_CACHE_FILE, "r") as f:
            cached = json.load(f)
            if time.time() < cached["expires_at"]:
                print("Using cached access token.")
                return cached["access_token"]

    print("Requesting new access token...")
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
def safe_get(url, headers, params, retries=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 10))
                print(f"Rate limit hit. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            elif response.status_code in [502, 503, 504]:
                print(f"Server error {response.status_code}. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            print(f"HTTP Error: {e}")
            break
    return None


# --- FETCH ANIMALS ---
def fetch_animals(token, limit=LIMIT, pages=MAX_PAGES):
    headers = {"Authorization": f"Bearer {token}"}
    all_animals = []

    for page in range(1, pages + 1):
        print(f"Fetching animal page {page}/{pages}...")
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
            print(f"No animals returned on page {page}. Stopping early.")
            break

        all_animals.extend(animals)

        if len(animals) < limit:
            print(f"Fewer than {limit} results on page {page}. Assuming end of data.")
            break

        time.sleep(0.2)

    print(f"Finished fetching animals. Total: {len(all_animals)}")
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
    print(f"Fetching organizations in {location}...")
    headers = {"Authorization": f"Bearer {token}"}
    all_orgs = []

    for page in range(1, pages + 1):
        print(f"Fetching organization page {page}/{pages}...")
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
            print(f"No organizations returned on page {page}. Stopping early.")
            break

        all_orgs.extend(orgs)

        if len(orgs) < limit:
            print(f"Fewer than {limit} results on page {page}. Assuming end of data.")
            break

        time.sleep(0.2)

    print(f"Finished fetching organizations. Total: {len(all_orgs)}")
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
    print(f"Saved {name_prefix} to {filename}")


def import_to_db(animal_path, org_path):
    # === Check if both files exist ===
    if not animal_path.exists() or not org_path.exists():
        raise FileNotFoundError("Missing one or both input files for today.")

    # === Load JSON Data ===
    with open(animal_path, "r", encoding="utf-8") as f:
        animal_data = json.load(f)

    with open(org_path, "r", encoding="utf-8") as f:
        org_data = json.load(f)

    # === Connect to SQLite DB ===
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # === Create Tables if Not Exist ===
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS organizations (
        id TEXT PRIMARY KEY,
        name TEXT,
        email TEXT,
        phone TEXT,
        city TEXT,
        state TEXT,
        postcode TEXT,
        website TEXT,
        url TEXT,
        photo TEXT
    );

    CREATE TABLE IF NOT EXISTS animals (
        id INTEGER PRIMARY KEY,
        name TEXT,
        type TEXT,
        species TEXT,
        primary_breed TEXT,
        secondary_breed TEXT,
        mixed INTEGER,
        age TEXT,
        gender TEXT,
        size TEXT,
        coat TEXT,
        spayed_neutered INTEGER,
        house_trained INTEGER,
        special_needs INTEGER,
        shots_current INTEGER,
        environment_children INTEGER,
        environment_dogs INTEGER,
        environment_cats INTEGER,
        tags TEXT,
        description TEXT,
        primary_photo TEXT,
        status TEXT,
        published_at TEXT,
        status_changed_at TEXT,
        organization_id TEXT,
        url TEXT,
        FOREIGN KEY (organization_id) REFERENCES organizations(id)
    );
    """)

    # === Upsert Organizations ===
    for org in org_data:
        cur.execute("""
            INSERT INTO organizations (id, name, email, phone, city, state, postcode, website, url, photo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                email=excluded.email,
                phone=excluded.phone,
                city=excluded.city,
                state=excluded.state,
                postcode=excluded.postcode,
                website=excluded.website,
                url=excluded.url,
                photo=excluded.photo
        """, (
            org.get("id"),
            org.get("name"),
            org.get("email"),
            org.get("phone"),
            org.get("address", {}).get("city"),
            org.get("address", {}).get("state"),
            org.get("address", {}).get("postcode"),
            org.get("website"),
            org.get("url"),
            org.get("photo")
        ))

    # === Upsert Animals ===
    for animal in animal_data:
        cur.execute("""
            INSERT INTO animals (
                id, name, type, species,
                primary_breed, secondary_breed, mixed,
                age, gender, size, coat,
                spayed_neutered, house_trained, special_needs, shots_current,
                environment_children, environment_dogs, environment_cats,
                tags, description, primary_photo, status,
                published_at, status_changed_at, organization_id, url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                type=excluded.type,
                species=excluded.species,
                primary_breed=excluded.primary_breed,
                secondary_breed=excluded.secondary_breed,
                mixed=excluded.mixed,
                age=excluded.age,
                gender=excluded.gender,
                size=excluded.size,
                coat=excluded.coat,
                spayed_neutered=excluded.spayed_neutered,
                house_trained=excluded.house_trained,
                special_needs=excluded.special_needs,
                shots_current=excluded.shots_current,
                environment_children=excluded.environment_children,
                environment_dogs=excluded.environment_dogs,
                environment_cats=excluded.environment_cats,
                tags=excluded.tags,
                description=excluded.description,
                primary_photo=excluded.primary_photo,
                status=excluded.status,
                published_at=excluded.published_at,
                status_changed_at=excluded.status_changed_at,
                organization_id=excluded.organization_id,
                url=excluded.url
        """, (
            animal.get("id"),
            animal.get("name"),
            animal.get("type"),
            animal.get("species"),
            animal.get("breeds", {}).get("primary"),
            animal.get("breeds", {}).get("secondary"),
            int(animal.get("breeds", {}).get("mixed", False)),
            animal.get("age"),
            animal.get("gender"),
            animal.get("size"),
            animal.get("coat"),
            int(animal.get("attributes", {}).get("spayed_neutered", False)),
            int(animal.get("attributes", {}).get("house_trained", False)),
            int(animal.get("attributes", {}).get("special_needs", False)),
            int(animal.get("attributes", {}).get("shots_current", False)),
            int(animal.get("environment", {}).get("children") is True),
            int(animal.get("environment", {}).get("dogs") is True),
            int(animal.get("environment", {}).get("cats") is True),
            ", ".join(animal.get("tags", [])),
            animal.get("description"),
            animal.get("primary_photo"),
            animal.get("status"),
            animal.get("published_at"),
            animal.get("status_changed_at"),
            animal.get("organization_id"),
            animal.get("url")
        ))

    # === Finalize and Close ===
    conn.commit()
    conn.close()

    print(f"Imported {len(animal_data)} animals and {len(org_data)} organizations for {today_str}.")


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

    import_to_db(animal_file, org_file)


if __name__ == "__main__":
    main()
