import json
import os
from datetime import datetime
import subprocess
import requests
import time

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

def initialize():
    folder = os.path.join(parent_path, "json_data")
    steam_file = os.path.join(folder, "steam.json")

    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Created json_data folder")

    if not os.path.exists(steam_file):
        with open(steam_file, "w", encoding='utf-8') as f:
            pass
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Created steam.json file")

def get_time():
    return int(time.time())

def update_steam_general_data(new_data, file):
    old_data = []
    if os.path.exists(file):
        if os.path.getsize(file) > 0:
            with open(file, "r", encoding="utf-8") as f:
                old_data = json.load(f)

    old_data_dict = {entry["appid"]: entry for entry in old_data}

    for app in new_data:
        appid = app["appid"]
        if appid in old_data_dict:
            old_entry = old_data_dict[appid]
            app["data"] = old_entry.get("data", [])
            if "last_updated" in old_entry:
                app["last_updated"] = old_entry["last_updated"]

        old_data_dict[appid] = app

    for app in old_data_dict.values():
        if "last_updated" not in app:
            app["last_updated"] = 0

    with open(file, "w", encoding="utf-8") as f:
        json.dump(list(old_data_dict.values()), f, indent=4)

def update_steam_app_data(new_app_data, old_general_data, file):
    old_data_dict = {entry["appid"]: entry for entry in old_general_data}
    old_data_dict[new_app_data["appid"]] = new_app_data

    updated_data = list(old_data_dict.values())

    with open(file, "w", encoding="utf-8") as f:
        json.dump(updated_data, f, indent=4)

def get_steam_data():
    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Started fetching Steam catalog")
    with open("credentials/steam_api_key.txt", 'r', encoding='utf-8') as f:
        steam_api_key = f.read().strip()

    modified_since = 0 # Default
    last_app_id = 0 # Default
    max_results = 50000   # MAX 50k

    # Fetch apps and general data --------------------------------------------------------------------------------------
    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Fetching apps general data...")
    while True:
        response_get_app_list = requests.get(f"https://api.steampowered.com/IStoreService/GetAppList/v1/?"
                                             f"key={steam_api_key}"
                                             f"&if_modified_since={modified_since}"
                                             f"&include_games=true"
                                             f"&last_appid={last_app_id}"
                                             f"&max_results={max_results}")

        if response_get_app_list.status_code == 200:
            apps_chunk = response_get_app_list.json()["response"]["apps"]
            update_steam_general_data(apps_chunk, os.path.join(parent_path, "json_data", "steam.json"))
            print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Fetched {len(apps_chunk)} app ids...")

            if len(apps_chunk) < max_results:
                print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Done fetching app ids")
                break
            else:
                last_app_id = apps_chunk[-1]["appid"]
        else:
            print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|ERROR|GetAppList request failed: {response_get_app_list.status_code}")
            break

    # Fetch detailed data from apps ------------------------------------------------------------------------------------
    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Fetching apps details...")
    with open(os.path.join(parent_path, 'json_data', 'steam.json'), 'r', encoding='utf-8') as f:
        steam_data = json.load(f)

    """
    Rate limits: 100.000reqs/day AND 200reqs/5min
    
    In the case of a 429 status code, the program can attempt to resend the request every 10 seconds until a 
    successful response is received. For a 403 status code, the program should wait for 5 minutes to comply with 
    the rate limit duration.
    """
    for app in steam_data:
        appid = app["appid"]
        if app["last_updated"] < app["last_modified"]:
            response_get_app_details = requests.get(f"https://store.steampowered.com/api/appdetails?appids={appid}")
            if response_get_app_details.status_code == 200:
                data = response_get_app_details.json().get(str(appid), {})
                if data.get("success"):
                    app["last_updated"] = get_time()
                    app["data"] = data["data"]
                    update_steam_app_data(app, steam_data, os.path.join(parent_path, "json_data", "steam.json"))
                    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|{response_get_app_details.status_code}|Fetched app {appid}...")
                else:
                    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|ERROR|{response_get_app_details.status_code}|Error fetching app details {appid}: Success = False ")
            elif response_get_app_details.status_code == 429:
                print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|ERROR|{response_get_app_details.status_code}|Error fetching app details {appid}: Too many requests")
                break
            elif response_get_app_details.status_code == 403:
                print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|ERROR|{response_get_app_details.status_code}|Error fetching app details {appid}: Forbidden")
                break
            else:
                print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|ERROR|{response_get_app_details.status_code}|Error fetching app details {appid}: Unknown failure")
                break
        else:
            print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Skipped app {appid}: Already up to date")

    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Stopped fetching app details")
    # ------------------------------------------------------------------------------------------------------------------
    print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|INFO|Ended fetching Steam catalog")

def run_crawler(mode):
    command = [
        "scrapy",
        "crawl",
        "no-scraper",
        "-a", "mode=" + mode,
    ]
    subprocess.run(command)

if __name__ == '__main__':
    initialize()
    get_steam_data()

    #run_crawler("epic")