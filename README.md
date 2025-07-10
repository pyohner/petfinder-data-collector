# Petfinder Data Collector

This Python script collects animal and organization data from the [Petfinder API](https://www.petfinder.com/developers/). It handles authentication, rate limits, data cleaning, and enrichment, and saves the results to local JSON files and a SQLite database.

## 🚀 Features

- Authenticates using API key and secret  
- Caches access token to avoid redundant requests  
- Handles API rate limiting (HTTP 429) and server errors  
- Cleans and filters raw animal and organization data  
- Matches each animal with its organization info  
- Outputs to date-stamped JSON files  
- Imports data into a SQLite database (path set via environment variable)

## 🛠 Setup

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Create a `.env` file** in the project root:

   ```env
   PETFINDER_API_KEY=your_petfinder_key
   PETFINDER_API_SECRET=your_petfinder_secret
   DATABASE_PATH=C:/path/to/petfinder_data.db
   ```

3. **Run the script:**

   ```bash
   python collect_petfinder_data.py
   ```

## 📁 Output

All files are saved to the `data_snapshots/` folder:

- `data_YYYY-MM-DD.json` – cleaned animal data  
- `organizations_YYYY-MM-DD.json` – cleaned organization data  
- `data_with_orgs_YYYY-MM-DD.json` – animal data enriched with organization info

The SQLite database is updated at the path specified in `DATABASE_PATH`.

## 💬 Related Repositories

- [Rescue Radar App (Frontend)](https://github.com/pyohner/rescue_radar_app)  
- [Rescue Radar API (Backend)](https://github.com/pyohner/rescue_radar_api)

## 🧾 Notes

- API usage is subject to daily and hourly limits. The script handles retry logic but may stop early if you exceed your quota.  
- Add `.env`, `token_cache.json`, and `data_snapshots/` to `.gitignore` to avoid committing secrets or large files.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.