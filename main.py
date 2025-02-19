import json
import os
import subprocess
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
    data_file = os.path.join(folder, "data.json")

    if not os.path.exists(folder):
        os.makedirs(folder)
        logger('INFO', 'Created json_data folder')

    if not os.path.exists(data_file):
        with open(data_file, "w", encoding='utf-8') as f:
            pass
        logger('INFO', 'Created data.json file')

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

    if "screenshots" in data:
        data["screenshots"] = [s["path_full"] for s in data["screenshots"] if "path_full" in s]

    if "movies" in data:
        data["movies"] = data["movies"][-3:]
        for movie in data["movies"]:
            for key in ["name", "webm", "mp4", "highlight"]:
                movie.pop(key, None)

    if "ratings" in data:
        pegi = data["ratings"].get("pegi", {})
        data["pegi"] = {
            "rating": pegi.get("rating", None),
            "descriptors": pegi.get("descriptors", None)
        }
    else:
        data["pegi"] = {"rating": None, "descriptors": None}
    data.pop("ratings", None)

    return data

def write_main_data(apps_chunk, file):
    """
    :param apps_chunk:
    :param file:
    :return:
    """

    old_apps = []
    default_store_json = {
        "availability": False,
        "price_in_cents": -1,
        "price_time": -1
    }
    if os.path.exists(file):
        if os.path.getsize(file) > 0:
            with open(file, "r", encoding="utf-8") as f:
                old_apps = json.load(f)

    old_apps_dict = {entry["appid"]: entry for entry in old_apps}

    for app in apps_chunk:
        appid = app["appid"]
        app.pop("price_change_number", None)
        if appid in old_apps_dict:
            old_entry = old_apps_dict[appid]
            if "last_fetched" in old_entry:
                app["last_fetched"] = old_entry["last_fetched"]
            if "steam" in old_entry:
                app["steam"] = old_entry["steam"]
            if "epic" in old_entry:
                app["epic"] = old_entry["epic"]
            if "ea" in old_entry:
                app["ea"] = old_entry["ea"]
            if "xbox" in old_entry:
                app["xbox"] = old_entry["xbox"]
            if "battle" in old_entry:
                app["battle"] = old_entry["battle"]
            if "rockstar" in old_entry:
                app["rockstar"] = old_entry["rockstar"]
            if "data" in old_entry:
                app["data"] = old_entry["data"]
        old_apps_dict[appid] = app

    for app in old_apps_dict.values():
        if "last_fetched" not in app:
            app["last_fetched"] = -1
        if "steam" not in app:
            app["steam"] = default_store_json
        if "epic" not in app:
            app["epic"] = default_store_json
        if "ea" not in app:
            app["ea"] = default_store_json
        if "xbox" not in app:
            app["xbox"] = default_store_json
        if "battle" not in app:
            app["battle"] = default_store_json
        if "rockstar" not in app:
            app["rockstar"] = default_store_json
        if "data" not in app:
            app["data"] = []

    with open(file, "w", encoding="utf-8") as f:
        json.dump(list(old_apps_dict.values()), f, indent=4)

def write_app_details(app_details_data, main_data, file):
    """
    :param app_details_data:
    :param main_data:
    :param file:
    :return:
    """
    main_data_dict = {entry["appid"]: entry for entry in main_data}
    main_data_dict[app_details_data["appid"]] = app_details_data
    main_data_updated = list(main_data_dict.values())

    with open(file, "w", encoding="utf-8") as f:
        json.dump(main_data_updated, f, indent=4)

def fetch_main_data():
    """
    :return:
    """
    logger('INFO','Started fetching Steam catalog')
    with open("credentials/steam_api_key.txt", 'r', encoding='utf-8') as f:
        steam_api_key = f.read().strip()

    modified_since = 0  # Default
    last_app_id = 0     # Default
    max_results = 50000 # MAX 50k

    while True:
        response_get_app_list = requests.get(f"https://api.steampowered.com/IStoreService/GetAppList/v1/?"
                                             f"key={steam_api_key}"
                                             f"&if_modified_since={modified_since}"
                                             f"&include_games=true"
                                             f"&last_appid={last_app_id}"
                                             f"&max_results={max_results}")

        if response_get_app_list.status_code == 200:
            apps_chunk = response_get_app_list.json()["response"]["apps"]
            write_main_data(apps_chunk, os.path.join(parent_path, "json_data", "data.json"))
            logger('INFO', f'Fetched {len(apps_chunk)} app ids')

            if len(apps_chunk) < max_results:
                break
            else:
                last_app_id = apps_chunk[-1]["appid"]
        else:
            logger('ERROR', f'GetAppList request failed: {response_get_app_list.status_code}')
            break
    logger('INFO', 'Ended fetching Steam catalog')

def fetch_apps_details():
    """
    Rate limits: 100.000reqs/day AND 200reqs/5min

    In the case of a 429 status code, the program can attempt to resend the request every 10 seconds until a
    successful response is received. For a 403 status code, the program should wait for 5 minutes to comply with
    the rate limit duration.

    :return:
    """
    logger('INFO', 'Started fetching apps details')
    with open(os.path.join(parent_path, 'json_data', 'data.json'), 'r', encoding='utf-8') as f:
        main_data = json.load(f)

    for app in main_data:
        appid = app["appid"]
        if app["last_fetched"] < app["last_modified"]:
            response_get_app_details = requests.get(f"https://store.steampowered.com/api/appdetails?appids={appid}")
            if response_get_app_details.status_code == 200:
                data = response_get_app_details.json().get(str(appid), {})
                if data.get("success"):
                    now = get_time()
                    app["last_fetched"] = now
                    app["steam"]["availability"] = True
                    app["steam"]["price_in_cents"] = 0 if data["data"]["is_free"] else data["data"]["price_overview"]["final"]
                    app["steam"]["price_time"] = now
                    app["data"] = remove_undesired_app_details(data["data"])
                    write_app_details(app, main_data, os.path.join(parent_path, "json_data", "data.json"))
                    logger('INFO', f'Fetched details for app {appid}', response_get_app_details.status_code)
                else:
                    logger('ERROR', f"Error fetching details for app {appid}: 'success' = False", response_get_app_details.status_code)
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
    logger('INFO', 'Ended fetching app details')

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
    initialize()
    fetch_main_data()
    fetch_apps_details()

    # TODO: WRITE DETAILS WHEN FINISHED EXECUTION
    # TODO: CREATE GENRES JSON
    # TODO: CREATE CATEGORIES JSON
    # TODO: CREATE DEVELOPERS JSON
    # TODO: CREATE PUBLISHERS JSON

    # Next step: run crawlers to get the remaining stores prices
    # run_crawler("epic")