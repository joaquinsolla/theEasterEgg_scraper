import json
import os
import subprocess
import traceback
import requests
import time
from datetime import datetime

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

def initialize():
    """
    Creates the needed set of folders and files for the execution.
    """
    folder = os.path.join(parent_path, "json_data")
    files = [
        os.path.join(folder, "games.json"),
        os.path.join(folder, "genres.json"),
        os.path.join(folder, "categories.json"),
        os.path.join(folder, "developers.json"),
        os.path.join(folder, "publishers.json"),
    ]

    if not os.path.exists(folder):
        os.makedirs(folder)
        logger('INFO', f'Created {folder} folder')

    for file in files:
        if not os.path.exists(file):
            with open(file, "w", encoding='utf-8') as f:
                pass
            logger('INFO', f'Created {file} file')

def logger(status, message, html_code=None):
    if html_code:
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{html_code}|{status}|{message}")
    else:
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{status}|{message}")

def get_time():
    """
    :return:
    """
    return int(time.time())

def set_steam_data(data):
    availability = False
    price_in_cents = -1
    price_time = get_time()

    if data["is_free"]:
        availability = True
        price_in_cents = 0
    elif "price_overview" in data:
        availability = True
        price_in_cents = data["price_overview"]["final"]

    return {
        "availability": availability,
        "price_in_cents": price_in_cents,
        "price_time": price_time
    }

def remove_undesired_app_details(data):
    """
    :param data:
    :return:
    """

    data.pop("steam_appid", None)
    data.pop("required_age", None)
    data.pop("detailed_description", None)
    data.pop("capsule_imagev5", None)
    data.pop("ext_user_account_notice", None)
    data.pop("price_overview", None)
    data.pop("packages", None)
    data.pop("package_groups", None)
    data.pop("achievements", None)
    data.pop("support_info", None)
    data.pop("background", None)
    data.pop("content_descriptors", None)

    if "screenshots" in data and data["screenshots"]:
        data["screenshots"] = [s["path_full"] for s in data["screenshots"] if "path_full" in s]
    else:
        data["screenshots"] = []

    if "movies" in data and data["movies"]:
        data["movies"] = data["movies"][-3:]
        for movie in data["movies"]:
            for key in ["name", "webm", "mp4", "highlight"]:
                movie.pop(key, None)
    else:
        data["movies"] = []

    if "ratings" in data and data["ratings"]:
        if "pegi" in data["ratings"] and data["ratings"]["pegi"]:
            pegi = data["ratings"].get("pegi", {})
            data["pegi"] = {
                "rating": pegi.get("rating", None),
                "descriptors": pegi.get("descriptors", None)
            }
        else:
            data["pegi"] = {"rating": None, "descriptors": None}
    else:
        data["pegi"] = {"rating": None, "descriptors": None}
    data.pop("ratings", None)

    return data

def update_games_catalog(games):
    """
    :param games:
    :return:
    """
    logger('INFO', 'Started updating games catalog')

    old_apps = []
    default_store_json = {
        "availability": False,
        "price_in_cents": -1,
        "price_time": -1
    }
    stores = {
        "steam": default_store_json,
        "epic": default_store_json,
        "ea": default_store_json,
        "xbox": default_store_json,
        "battle": default_store_json,
        "rockstar": default_store_json,
    }
    if os.path.getsize(os.path.join(parent_path, "json_data", "games.json")) > 0:
        with open(os.path.join(parent_path, "json_data", "games.json"), "r", encoding="utf-8") as f:
            old_apps = json.load(f)

    old_apps_dict = {entry["appid"]: entry for entry in old_apps}

    for app in games:
        appid = app["appid"]
        app.pop("price_change_number", None)
        if appid in old_apps_dict:
            old_entry = old_apps_dict[appid]
            if "last_fetched" in old_entry:
                app["last_fetched"] = old_entry["last_fetched"]
            if "stores" in old_entry:
                app["stores"] = old_entry["stores"]
            if "data" in old_entry:
                app["data"] = old_entry["data"]
        old_apps_dict[appid] = app

    for app in old_apps_dict.values():
        if "last_fetched" not in app:
            app["last_fetched"] = -1
        if "stores" not in app:
            app["stores"] = stores
        if "data" not in app:
            app["data"] = []

    with open(os.path.join(parent_path, "json_data", "games.json"), "w", encoding="utf-8") as f:
        json.dump(list(old_apps_dict.values()), f, indent=4)
    logger('INFO', 'Ended updating games catalog')

def update_games_details(games):
    """
    :param games:
    :return:
    """
    logger('INFO', 'Started updating games details')
    with open(os.path.join(parent_path, "json_data", "games.json"), "w", encoding="utf-8") as f:
        json.dump(games, f, indent=4)
    logger('INFO', 'Ended updating games details')

def fetch_games_catalog():
    """
    :return:
    """
    logger('INFO','Started fetching Steam catalog')
    with open("credentials/steam_api_key.txt", 'r', encoding='utf-8') as f:
        steam_api_key = f.read().strip()

    modified_since = 0  # Default
    last_app_id = 0     # Default
    max_results = 50000 # MAX 50k
    apps = []

    while True:
        response_get_app_list = requests.get(f"https://api.steampowered.com/IStoreService/GetAppList/v1/?"
                                             f"key={steam_api_key}"
                                             f"&if_modified_since={modified_since}"
                                             f"&include_games=true"
                                             f"&last_appid={last_app_id}"
                                             f"&max_results={max_results}")

        if response_get_app_list.status_code == 200:
            apps_chunk = response_get_app_list.json()["response"]["apps"]
            apps.extend(apps_chunk)
            if len(apps_chunk) < max_results:
                update_games_catalog(apps)
                break
            else:
                last_app_id = apps_chunk[-1]["appid"]
        else:
            logger('ERROR', f'GetAppList request failed: {response_get_app_list.status_code}')
            break
    logger('INFO', 'Ended fetching Steam catalog')

def fetch_games_details():
    """
    Rate limits: 100.000reqs/day AND 200reqs/5min

    In the case of a 429 status code, the program can attempt to resend the request every 10 seconds until a
    successful response is received. For a 403 status code, the program should wait for 5 minutes to comply with
    the rate limit duration.

    :return:
    """
    logger('INFO', 'Started fetching games details')
    with open(os.path.join(parent_path, 'json_data', 'games.json'), 'r', encoding='utf-8') as f:
        games = json.load(f)

    try:
        for app in games:
            start_time = time.time()
            appid = app["appid"]
            if app["last_fetched"] < app["last_modified"]:
                response_get_app_details = requests.get(f"https://store.steampowered.com/api/appdetails?appids={appid}")
                if response_get_app_details.status_code == 200:
                    data = response_get_app_details.json().get(str(appid), {})
                    if data.get("success"):
                        app["last_fetched"] = get_time()
                        app["stores"]["steam"] = set_steam_data(data["data"])
                        app["data"] = remove_undesired_app_details(data["data"])
                        logger('INFO', f'Fetched details for app {appid}', response_get_app_details.status_code)
                    else:
                        logger('INFO', f'Cannot fetch details for app {appid}: Not available', response_get_app_details.status_code)

                    elapsed_time = time.time() - start_time
                    remaining_time = 1.51 - elapsed_time
                    if remaining_time > 0:
                        time.sleep(remaining_time)

                elif response_get_app_details.status_code == 429:
                    logger('ERROR', f'Error fetching details for app {appid}: Too many requests', response_get_app_details.status_code)
                    break
                elif response_get_app_details.status_code == 403:
                    logger('ERROR', f'Error fetching details for app {appid}: Forbidden', response_get_app_details.status_code)
                    break
                else:
                    logger('ERROR', f'Error fetching details for app {appid}: Unknown error', response_get_app_details.status_code)
                    break
            else:
                logger('INFO', f'Skipped app {appid}: Already up to date')
    except:
        logger('ERROR', traceback.format_exc(), f'appid={appid}')

    logger('INFO', 'Ended fetching games details')
    update_games_details(games)

def run_crawler(mode):
    """
    :param mode:
    :return:
    """

    command = [
        "scrapy",
        "crawl",
        "no-scraper",
        "-a", "mode=" + mode,
    ]
    subprocess.run(command)

if __name__ == '__main__':
    try:
        initialize()
        fetch_games_catalog()
        fetch_games_details()
    except:
        logger('ERROR', traceback.format_exc())



    # TODO: CREATE GENRES JSON
    # TODO: CREATE CATEGORIES JSON
    # TODO: CREATE DEVELOPERS JSON
    # TODO: CREATE PUBLISHERS JSON

    # Next step: run crawlers to get the remaining stores prices
    # run_crawler('epic')