import json
import os
import re
import subprocess
import traceback
import requests
import time
import gzip
import shutil
from datetime import datetime
from epicstore_api import EpicGamesStoreAPI
import xml.etree.ElementTree as ET

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

def initialize():
    """
    Creates the needed set of folders and files for the execution.
    """
    json_data_folder = os.path.join(parent_path, "json_data")
    ndjson_data_folder = os.path.join(parent_path, "ndjson_data")
    xml_sitemaps_folder = os.path.join(parent_path, "xml_sitemaps")
    xbox_sitemaps_folder = os.path.join(parent_path, "xml_sitemaps", "xbox")
    json_temp_folder = os.path.join(parent_path, "json_data", "temp")

    folders = [
        json_data_folder,
        ndjson_data_folder,
        xml_sitemaps_folder,
        xbox_sitemaps_folder,
        json_temp_folder
    ]

    for folder in folders:
        if not os.path.exists(folder):
            os.makedirs(folder)
            logger('INFO', f'Created {folder}')

    files = [
        os.path.join(json_data_folder, "fetching_info.json"),
        os.path.join(json_data_folder, "games.json"),
        os.path.join(json_data_folder, "genres.json"),
        os.path.join(json_data_folder, "categories.json"),
        os.path.join(json_data_folder, "developers.json"),
        os.path.join(json_data_folder, "publishers.json"),
        os.path.join(json_data_folder, "pegi.json"),
        os.path.join(xml_sitemaps_folder, "xbox.xml"),
        os.path.join(xml_sitemaps_folder, "battle.xml"),
        os.path.join(xml_sitemaps_folder, "gog.xml"),
        os.path.join(json_temp_folder, "xbox_coincidences.json"),
        os.path.join(json_temp_folder, "battle_coincidences.json"),
        os.path.join(json_temp_folder, "gog_coincidences.json"),
        os.path.join(json_data_folder, "prices_history.json")
    ]

    for file in files:
        if not os.path.exists(file):
            with open(file, "w", encoding='utf-8') as f:
                pass
            logger('INFO', f'Created {file}')

    logger('INFO', 'Initialization done')

def finalize(error=None):
    old_data = read_json("fetching_info.json")
    execution = 1
    if "exec_no" in old_data:
        execution = old_data["exec_no"]+1
    new_data = {
        "exec_no": execution,
        "time": get_time(),
        "error": error
    }
    write_json("fetching_info.json", new_data)

def read_json(filename):
    file_path = os.path.join(parent_path, 'json_data', filename)

    if not os.path.exists(file_path):
        return []

    if os.path.getsize(file_path) == 0:
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger(f'ERROR', f'Cannot read file {filename}')
        return []

def write_json(filename, data):
    with open(os.path.join(parent_path, "json_data", filename), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def logger(status, message, html_code=None):
    if html_code:
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{status}|{html_code}|{message}")
    else:
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{status}|{message}")

def get_time():
    """
    :return:
    """
    return int(time.time())

def iso_time_to_unix_time(timestamp: str) -> int:
    """
    Convierte una fecha en formato ISO 8601 (UTC) a UNIX timestamp.
    """
    return int(datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").timestamp())

def get_url_name(name):
    name = name.strip().lower()
    name = re.sub(r'[^a-z0-9 ]', '', name)
    name = name.replace(' ', '-')
    return name

def get_steam_data(data):
    availability = False
    price_in_cents = None
    price_time = get_time()
    url = None

    if data["is_free"]:
        availability = True
        price_in_cents = 0
    elif "price_overview" in data:
        availability = True
        price_in_cents = data["price_overview"]["final"]

    if availability:
        url = "https://store.steampowered.com/app/" + str(data["steam_appid"])

    return {
        "availability": availability,
        "price_in_cents": price_in_cents,
        "price_time": price_time,
        "url": url
    }

def get_metacritic_data(data):
    metacritic = {
        "scale": None,
        "score": None,
        "url": None,
        "last_fetched": -1
    }

    if "metacritic" in data and data["metacritic"]:
        metacritic = {
            "scale": 100,
            "score": data["metacritic"]["score"] if "score" in data["metacritic"] else None,
            "url": data["metacritic"]["url"] if "url" in data["metacritic"] else None,
            "last_fetched": get_time()
        }

    return metacritic

def extract_year(date_str):
    date_formats = ["%d %b, %Y", "%b %d, %Y"]

    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).year
        except ValueError:
            continue
    return None

def clean_app_details(data):
    """
    :param data:
    :return:
    """
    data.pop("steam_appid", None)
    data.pop("name", None)
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
    data.pop("metacritic", None)
    data.pop("controller_support", None)
    data.pop("dlc", None)
    data.pop("reviews", None)

    if "developers" not in data:
        data["developers"] = []

    if "publishers" not in data:
        data["publishers"] = []

    if "genres" not in data:
        data["genres"] = []
    else:
        data["genres"] = [item["description"] for item in data["genres"]]

    if "categories" not in data:
        data["categories"] = []
    else:
        data["categories"] = [item["description"] for item in data["categories"]]

    if "platforms" not in data:
        data["availability_windows"] = False
        data["availability_mac"] = False
        data["availability_linux"] = False
    else:
        data["availability_windows"] = data["platforms"]["windows"] if "windows" in data["platforms"] else False
        data["availability_mac"] = data["platforms"]["mac"] if "mac" in data["platforms"] else False
        data["availability_linux"] = data["platforms"]["linux"] if "linux" in data["platforms"] else False
        data.pop("platforms", None)

    if "recommendations" not in data:
        data["total_recommendations"] = 0
    else:
        data["total_recommendations"] = data["recommendations"]["total"] if "total" in data["recommendations"] else 0
        data.pop("recommendations", None)

    if "release_date" not in data:
        data["release_date"] = None
    else:
        if "date" in data["release_date"]:
            try:
                year = extract_year(data["release_date"]["date"])
                if year:
                    data["release_date"]["year"] = year
                else:
                    data["release_date"]["year"] = None
                data["release_date"]["date"] = int(
                    datetime.strptime(data["release_date"]["date"], "%d %b, %Y").timestamp())
            except ValueError:
                data["release_date"]["date"] = None
        else:
            data["release_date"]["date"] = None

    if "pc_requirements" not in data or not data["pc_requirements"]:
        data["pc_requirements"] = None
    if "mac_requirements" not in data or not data["mac_requirements"]:
        data["mac_requirements"] = None
    if "linux_requirements" not in data or not data["linux_requirements"]:
        data["linux_requirements"] = None

    # Screenshots
    if "screenshots" in data and data["screenshots"]:
        data["screenshots"] = [s["path_full"] for s in data["screenshots"] if "path_full" in s][::-1]
    else:
        data["screenshots"] = []

    # Movies
    if "movies" in data and data["movies"]:
        data["movies"] = data["movies"][-3:][::-1]
        for movie in data["movies"]:
            for key in ["name", "webm", "mp4", "highlight"]:
                movie.pop(key, None)
    else:
        data["movies"] = []

    # Ratings
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
        "price_in_cents": None,
        "price_time": None,
        "url": None
    }
    stores = {
        "steam": default_store_json,
        "epic": default_store_json,
        "xbox": default_store_json,
        "battle": default_store_json,
        "gog": default_store_json,
    }
    default_critic_json = {
        "scale": None,
        "score": None,
        "url": None,
        "last_fetched": -1
    }
    #critics = {
    #    "metacritic": default_critic_json,
    #    "opencritic": default_critic_json,
    #}
    if os.path.getsize(os.path.join(parent_path, "json_data", "games.json")) > 0:
        old_apps = read_json('games.json')

    old_apps_dict = {entry["appid"]: entry for entry in old_apps}

    for app in games:
        appid = app["appid"]
        app.pop("price_change_number", None)
        if appid in old_apps_dict:
            old_entry = old_apps_dict[appid]
            if "last_fetched" in old_entry:
                app["last_fetched"] = old_entry["last_fetched"]
            if "url_name" in old_entry:
                app["url_name"] = old_entry["url_name"]
            if "stores" in old_entry:
                app["stores"] = old_entry["stores"]
            if "metacritic" in old_entry:
                app["metacritic"] = old_entry["metacritic"]
            if "data" in old_entry:
                app["data"] = old_entry["data"]
        old_apps_dict[appid] = app

    for app in old_apps_dict.values():
        if "last_fetched" not in app:
            app["last_fetched"] = -1
        if "url_name" not in app:
            app["url_name"] = get_url_name(app["name"])
        if "stores" not in app:
            app["stores"] = stores
        if "metacritic" not in app:
            app["metacritic"] = default_critic_json
        if "data" not in app:
            app["data"] = []

    write_json('games.json', list(old_apps_dict.values()))
    logger('INFO', 'Ended updating games catalog')

def update_prices_history(games):
    """
    :param games:
    :return:
    """
    logger('INFO', 'Started updating prices history')

    old_history = []

    if os.path.getsize(os.path.join(parent_path, "json_data", "prices_history.json")) > 0:
        old_history = read_json('prices_history.json')

    old_history_dict = {entry["appid"]: entry for entry in old_history}

    for app in games:
        appid = app["appid"]
        if appid not in old_history_dict:
            old_history.append({
                "appid": appid,
                "steam": [],
                "epic": [],
                "xbox": [],
                "battle": [],
                "gog": [],
            })

    write_json('prices_history.json', old_history)
    logger('INFO', 'Ended updating prices history')

def process_xbox_sitemaps():
    try:
        download_xml_sitemap('https://www.xbox.com/sitemap.xml', 'xbox.xml')

        with open(os.path.join(parent_path, "xml_sitemaps", 'xbox.xml'), "r", encoding="utf-8") as file:
            xml_data = file.read()

        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        root = ET.fromstring(xml_data)

        sitemaps = [
            {
                "loc": sm.find("ns:loc", namespace).text,
                "lastmod": iso_time_to_unix_time(sm.find("ns:lastmod", namespace).text)
            }
            for sm in root.findall("ns:sitemap", namespace)
            if "es-ES" in sm.find("ns:loc", namespace).text and "xcloud" not in sm.find("ns:loc", namespace).text
        ]

        fetch_info = read_json('fetch_info.json')

        x = 0
        compressed_files = []
        for sitemap in sitemaps:
            if not fetch_info or (fetch_info and "last_fetch" in fetch_info and sitemap["lastmod"] > fetch_info["last_fetch"]):

                url = sitemap["loc"]

                output_folder = os.path.join(parent_path, "xml_sitemaps", "xbox")
                compressed_file = os.path.join(output_folder, f"xbox-compressed-{x}.gz")
                decompressed_file = os.path.join(output_folder, f"xbox-{x}.xml")

                response = requests.get(url, stream=True)
                if response.status_code == 200:
                    with open(compressed_file, "wb") as file:
                        shutil.copyfileobj(response.raw, file)
                    compressed_files.append(compressed_file)
                    logger('INFO', f'Downloaded {compressed_file}', 200)
                else:
                    logger('ERROR', f'Cannot download {compressed_file}', response.status_code)
                    exit()

                with gzip.open(compressed_file, "rb") as f_in:
                    with open(decompressed_file, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
            x += 1

        for file in compressed_files:
            os.remove(file)

    except:
        logger('ERROR', traceback.format_exc())

def process_battle_sitemaps():
    try:
        download_xml_sitemap('https://eu.shop.battle.net/sitemap_es-es.xml', 'battle.xml')

        with open(os.path.join(parent_path, "xml_sitemaps", 'battle.xml'), "r", encoding="utf-8") as file:
            xml_data = file.read()

        root = ET.fromstring(xml_data)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        catalog = []
        for url in root.findall('ns:url', namespace):
            loc = url.find('ns:loc', namespace).text
            lastmod = url.find('ns:lastmod', namespace).text

            if '/product/' in loc:
                dt = datetime.fromisoformat(lastmod)
                seconds = int(dt.timestamp())
                url_name = loc.split('/product/')[-1]

                catalog.append({
                    "url": loc,
                    "lastmod": seconds,
                    "url_name": url_name
                })

        return catalog

    except:
        logger('ERROR', traceback.format_exc())

def process_gog_sitemaps():
    try:
        download_xml_sitemap('https://www.gog.com/sitemap_en.xml', 'gog.xml')

        with open(os.path.join(parent_path, "xml_sitemaps", 'gog.xml'), "r", encoding="utf-8") as file:
            xml_data = file.read()

        root = ET.fromstring(xml_data)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        catalog = []
        for url in root.findall('ns:url', namespace):
            loc = url.find('ns:loc', namespace).text
            lastmod = url.find('ns:lastmod', namespace).text

            if '/game/' in loc:
                dt = datetime.strptime(lastmod, "%Y-%m-%d")
                seconds = int(dt.timestamp())
                url_name = loc.split('/game/')[-1].replace('_', '-')

                catalog.append({
                    "url": loc,
                    "lastmod": seconds,
                    "url_name": url_name
                })

        return catalog

    except:
        logger('ERROR', traceback.format_exc())

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

def download_xml_sitemap(xml_url, filename):
    response = requests.get(xml_url)

    if response.status_code == 200:
        with open(os.path.join(parent_path, 'xml_sitemaps', filename), 'wb') as file:
            file.write(response.content)
        logger('INFO', f"Downloaded {filename}", 200)
    else:
        logger('ERROR', f"Cannot download {filename}", response.status_code)

def build_xbox_catalog():
    logger('INFO', "Started updating Xbox catalog")

    xbox_path = os.path.join(parent_path, "xml_sitemaps", "xbox")
    catalog = []

    try:
        for filename in os.listdir(xbox_path):
            if filename.endswith('.xml'):
                file_path = os.path.join(xbox_path, filename)

                tree = ET.parse(file_path)
                root = tree.getroot()

                for url in root.findall('ns:url', namespaces={"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}):
                    loc = url.find('ns:loc', namespaces={"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}).text
                    lastmod = url.find('ns:lastmod', namespaces={"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}).text

                    catalog.append({
                        'url': loc,
                        'lastmod': lastmod,
                        'url_name': loc.split("store/")[1].split("/")[0],
                        'price_in_cents': None,
                        'price_time': None
                    })

        logger('INFO', "Ended updating Xbox catalog")
    except:
        logger('ERROR', traceback.format_exc())

    return catalog

def fetch_steam_catalog():
    """
    :param limit:
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
                update_prices_history(apps)
                break
            else:
                last_app_id = apps_chunk[-1]["appid"]
        else:
            logger('ERROR', f'GetAppList request failed: {response_get_app_list.status_code}')
            break
    logger('INFO', 'Ended fetching Steam catalog')

def fetch_steam_catalog_by_ids(ids_list):
    """
    FOR TEST PURPOSES
    :return:
    """
    logger('INFO','Started fetching Steam catalog')
    with open("credentials/steam_api_key.txt", 'r', encoding='utf-8') as f:
        steam_api_key = f.read().strip()

    apps = []
    for game_id in ids_list:
        response_get_app_list = requests.get(f"https://api.steampowered.com/IStoreService/GetAppList/v1/?"
                                             f"key={steam_api_key}"
                                             f"&include_games=true"
                                             f"&last_appid={game_id - 1}"
                                             f"&max_results={1}")

        if response_get_app_list.status_code == 200:
            apps_chunk = response_get_app_list.json()["response"]["apps"]
            apps.extend(apps_chunk)
            if len(apps) >= len(ids_list):
                update_games_catalog(apps)
                update_prices_history(apps)
                break
        else:
            logger('ERROR', f'GetAppList request failed: {response_get_app_list.status_code}')
            break
    logger('INFO', 'Ended fetching Steam catalog')

def fetch_steam_details(limit=None):
    """
    Rate limits: 100.000reqs/day AND 200reqs/5min

    In the case of a 429 status code, the program can attempt to resend the request every 10 seconds until a
    successful response is received. For a 403 status code, the program should wait for 5 minutes to comply with
    the rate limit duration.

    :return:
    """
    logger('INFO', 'Started fetching games details')
    games = read_json('games.json')
    genres = read_json('genres.json')
    categories = read_json('categories.json')
    developers = read_json('developers.json')
    publishers = read_json('publishers.json')
    pegi = read_json('pegi.json')
    prices_history = read_json('prices_history.json')
    prices_history_dict = {entry["appid"]: entry for entry in prices_history}
    count = 0

    try:
        for app in games:
            start_time = time.time()
            appid = app["appid"]
            if app["last_fetched"] < app["last_modified"] and (limit is None or count <= limit):
                count += 1
                response_get_app_details = requests.get(f"https://store.steampowered.com/api/appdetails?appids={appid}")
                if response_get_app_details.status_code == 200:
                    data = response_get_app_details.json().get(str(appid), {})
                    if data.get("success"):
                        # Games
                        app["last_fetched"] = get_time()
                        app["stores"]["steam"] = get_steam_data(data["data"])
                        #app["critics"]["metacritic"] = get_metacritic_data(data["data"])
                        app["metacritic"] = get_metacritic_data(data["data"])
                        app["data"] = clean_app_details(data["data"])
                        logger('INFO', f'Fetched details for app {appid}', response_get_app_details.status_code)
                        # Genres
                        if "genres" in app["data"]:
                            genres.extend(item for item in app["data"]["genres"]if item not in genres)
                        # Categories
                        if "categories" in app["data"]:
                            categories.extend(item for item in app["data"]["categories"]if item not in categories)
                        # Developers
                        if "developers" in app["data"]:
                            developers.extend(item for item in app["data"]["developers"] if item not in developers)
                        # Publishers
                        if "publishers" in app["data"]:
                            publishers.extend(item for item in app["data"]["publishers"] if item not in publishers)
                        # PEGI
                        if "pegi" in app["data"] and "rating" in app["data"]["pegi"]:
                            rating = app["data"]["pegi"]["rating"]
                            if rating is not None:
                                if isinstance(rating, list):
                                    pegi.extend(item for item in rating if item not in pegi)
                                else:
                                    if rating not in pegi:
                                        pegi.append(rating)

                        # Prices history (Steam)
                        if app["stores"]["steam"]["price_in_cents"] is not None and app["stores"]["steam"]["price_in_cents"] >= 0:
                            if appid in prices_history_dict:
                                prices_history_dict[appid]["steam"].append({
                                    "price_in_cents": app["stores"]["steam"]["price_in_cents"],
                                    "price_time": app["stores"]["steam"]["price_time"],
                                })

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
            elif limit is not None and count > limit:
                logger('INFO', f'Reached manual fetching limit of {limit}')
                break
            else:
                # Prices history (Steam)
                if app["stores"]["steam"]["price_in_cents"] is not None and app["stores"]["steam"]["price_in_cents"] >= 0:
                    if appid in prices_history_dict:
                        prices_history_dict[appid]["steam"].append({
                            "price_in_cents": app["stores"]["steam"]["price_in_cents"],
                            "price_time": get_time(),
                        })

                logger('INFO', f'Skipped app {appid}: Already up to date')
    except:
        logger('ERROR', traceback.format_exc(), f'appid={appid}')

    logger('INFO', 'Ended fetching games details')

    logger('INFO', 'Started updating JSON files')
    write_json('games.json', games)
    write_json('genres.json', genres)
    write_json('categories.json', categories)
    write_json('developers.json', developers)
    write_json('publishers.json', publishers)
    write_json('pegi.json', pegi)
    write_json('prices_history.json', list(prices_history_dict.values()))
    logger('INFO', 'Ended updating JSON files')

def fetch_epic_catalog():
    logger('INFO', 'Started fetching Epic Games catalog')

    games = read_json('games.json')
    prices_history = read_json('prices_history.json')
    prices_history_dict = {entry["appid"]: entry for entry in prices_history}
    url_names = []
    for game in games:
        url_names.append(game["url_name"])

    api = EpicGamesStoreAPI(locale='es-ES', country='ES')
    epic_catalog = []
    coincidences = []
    start = 0
    items_per_request = 40 # New API limit

    try:
        while True:
            response = api.fetch_store_games(count=items_per_request, start=start, allow_countries='ES', with_price=True)
            if "data" in response and "Catalog" in response["data"] and "searchStore" in response["data"]["Catalog"] and "elements" in response["data"]["Catalog"]["searchStore"]:
                games_chunk = response["data"]["Catalog"]["searchStore"]["elements"]
                epic_catalog.extend(games_chunk)
                start += items_per_request
                if len(games_chunk) < items_per_request:
                    break
            else:
                break
        logger('INFO', 'Ended fetching Epic Games catalog')

    except:
        logger('ERROR', traceback.format_exc())

    try:
        logger('INFO', 'Searching for coincidences between Steam and Epic catalogs')
        for game in epic_catalog:
            if "title" in game and game["title"] is not None:
                if "productSlug" in game and game["productSlug"] is not None:
                    if "price" in game and game["price"] is not None:
                        if "totalPrice" in game["price"] and game["price"]["totalPrice"] is not None:
                            if "discountPrice" in game["price"]["totalPrice"] and game["price"]["totalPrice"]["discountPrice"] is not None:
                                if game["productSlug"] in url_names:
                                    coincidences.append({
                                        "url_name": game["productSlug"],
                                        "price_in_cents": game["price"]["totalPrice"]["discountPrice"],
                                    })
                                elif get_url_name(game["title"]) in url_names:
                                    coincidences.append({
                                        "url_name": get_url_name(game["title"]),
                                        "price_in_cents": game["price"]["totalPrice"]["discountPrice"],
                                    })

        coincidences_dict = {coincidence["url_name"]: coincidence["price_in_cents"] for coincidence in coincidences}

        logger('INFO', f'{len(coincidences_dict)} coincidences found')
        logger('INFO', 'Started updating JSON files')

        for game in games:
            if game["url_name"] in coincidences_dict:
                game["stores"]["epic"]["availability"] = True
                game["stores"]["epic"]["price_in_cents"] = coincidences_dict[game["url_name"]]
                game["stores"]["epic"]["price_time"] = get_time()
                game["stores"]["epic"]["url"] = "https://store.epicgames.com/es-ES/p/" + game["url_name"]
                # Prices history (Epic)
                if game["stores"]["epic"]["price_in_cents"] is not None and game["stores"]["epic"]["price_in_cents"] >= 0:
                    if game["appid"] in prices_history_dict:
                        prices_history_dict[game["appid"]]["epic"].append({
                            "price_in_cents": game["stores"]["epic"]["price_in_cents"],
                            "price_time": game["stores"]["epic"]["price_time"],
                        })
            else:
                game["stores"]["epic"]["availability"] = False
                game["stores"]["epic"]["price_in_cents"] = None
                game["stores"]["epic"]["price_time"] = get_time()
                game["stores"]["epic"]["url"] = None

        if len(coincidences) > 0:
            write_json('games.json', games)
            write_json('prices_history.json', list(prices_history_dict.values()))
        logger('INFO', 'Ended updating JSON files')
    except:
        logger('ERROR', traceback.format_exc())

def fetch_xbox_catalog():
    """
    :return:
    """
    process_xbox_sitemaps()
    xbox_catalog = build_xbox_catalog()

    games = read_json('games.json')
    prices_history = read_json('prices_history.json')
    prices_history_dict = {entry["appid"]: entry for entry in prices_history}
    url_names = []
    for game in games:
        url_names.append(game["url_name"])

    coincidences = []

    logger('INFO', 'Searching for coincidences between Steam and Xbox catalogs')

    for game in xbox_catalog:
        if game["url_name"] in url_names:
            coincidences.append(game)

    logger('INFO', f'{len(coincidences)} coincidences found')

    write_json(os.path.join("temp", "xbox_coincidences.json"), coincidences)

    logger('INFO', 'Started crawling Xbox prices')
    try:
        run_crawler('xbox')
    except:
        logger('ERROR', traceback.format_exc())
    logger('INFO', 'Ended crawling Xbox prices')

    logger('INFO', 'Started updating Xbox prices')
    xbox_coincidences = read_json(os.path.join("temp", "xbox_coincidences.json"))
    xbox_coincidences_dict = {coincidence["url_name"]: coincidence for coincidence in xbox_coincidences}

    for game in games:
        if game["url_name"] in xbox_coincidences_dict:
            if xbox_coincidences_dict[game["url_name"]]["price_in_cents"] != None:
                game["stores"]["xbox"]["availability"] = True
                game["stores"]["xbox"]["price_in_cents"] = xbox_coincidences_dict[game["url_name"]]["price_in_cents"]
                game["stores"]["xbox"]["price_time"] = xbox_coincidences_dict[game["url_name"]]["price_time"]
                game["stores"]["xbox"]["url"] = xbox_coincidences_dict[game["url_name"]]["url"]
                # Prices history (Xbox)
                if game["stores"]["xbox"]["price_in_cents"] is not None and game["stores"]["xbox"]["price_in_cents"] >= 0:
                    if game["appid"] in prices_history_dict:
                        prices_history_dict[game["appid"]]["xbox"].append({
                            "price_in_cents": game["stores"]["xbox"]["price_in_cents"],
                            "price_time": game["stores"]["xbox"]["price_time"],
                        })
            else:
                game["stores"]["xbox"]["availability"] = False
                game["stores"]["xbox"]["price_in_cents"] = xbox_coincidences_dict[game["url_name"]]["price_in_cents"]
                game["stores"]["xbox"]["price_time"] = get_time()
                game["stores"]["xbox"]["url"] = None
        else:
            game["stores"]["xbox"]["availability"] = False
            game["stores"]["xbox"]["price_in_cents"] = None
            game["stores"]["xbox"]["price_time"] = get_time()
            game["stores"]["xbox"]["url"] = None

    if len(xbox_coincidences_dict) > 0:
        write_json('games.json', games)
        write_json('prices_history.json', list(prices_history_dict.values()))

    logger('INFO', 'Ended updating Xbox prices')

def fetch_battle_catalog():
    games = read_json('games.json')
    prices_history = read_json('prices_history.json')
    prices_history_dict = {entry["appid"]: entry for entry in prices_history}
    url_names = []
    for game in games:
        url_names.append(game["url_name"])
    coincidences = []

    battle_catalog = process_battle_sitemaps()

    logger('INFO', 'Searching for coincidences between Steam and Battle.net catalogs')

    for game in battle_catalog:
        url_name = game["url_name"]
        if url_name in url_names:
            coincidences.append({
                'url': game["url"],
                'url_name': url_name,
                'price_in_cents': None,
                'price_time': None
            })

    write_json(os.path.join("temp", "battle_coincidences.json"), coincidences)

    logger('INFO', f'{len(coincidences)} coincidences found')

    logger('INFO', 'Started crawling Battle.net prices')
    try:
        run_crawler('battle')
    except:
        logger('ERROR', traceback.format_exc())
    logger('INFO', 'Ended crawling Battle.net prices')

    logger('INFO', 'Started updating Battle.net prices')
    battle_coincidences = read_json(os.path.join("temp", "battle_coincidences.json"))
    battle_coincidences_dict = {coincidence["url_name"]: coincidence for coincidence in battle_coincidences}

    for game in games:
        if game["url_name"] in battle_coincidences_dict:
            if battle_coincidences_dict[game["url_name"]]["price_in_cents"] != None:
                game["stores"]["battle"]["availability"] = True
                game["stores"]["battle"]["price_in_cents"] = battle_coincidences_dict[game["url_name"]]["price_in_cents"]
                game["stores"]["battle"]["price_time"] = battle_coincidences_dict[game["url_name"]]["price_time"]
                game["stores"]["battle"]["url"] = battle_coincidences_dict[game["url_name"]]["url"]
                # Prices history (Battle)
                if game["stores"]["battle"]["price_in_cents"] is not None and game["stores"]["battle"]["price_in_cents"] >= 0:
                    if game["appid"] in prices_history_dict:
                        prices_history_dict[game["appid"]]["battle"].append({
                            "price_in_cents": game["stores"]["battle"]["price_in_cents"],
                            "price_time": game["stores"]["battle"]["price_time"],
                        })
            else:
                game["stores"]["battle"]["availability"] = False
                game["stores"]["battle"]["price_in_cents"] = None
                game["stores"]["battle"]["price_time"] = get_time()
                game["stores"]["battle"]["url"] = None
        else:
            game["stores"]["battle"]["availability"] = False
            game["stores"]["battle"]["price_in_cents"] = None
            game["stores"]["battle"]["price_time"] = get_time()
            game["stores"]["battle"]["url"] = None

    if len(battle_coincidences_dict) > 0:
        write_json('games.json', games)
        write_json('prices_history.json', list(prices_history_dict.values()))

    logger('INFO', 'Ended updating Battle.net prices')

def fetch_gog_catalog():
    games = read_json('games.json')
    prices_history = read_json('prices_history.json')
    prices_history_dict = {entry["appid"]: entry for entry in prices_history}
    url_names = []
    for game in games:
        url_names.append(game["url_name"])
    coincidences = []

    gog_catalog = process_gog_sitemaps()

    logger('INFO', 'Searching for coincidences between Steam and gog.com catalogs')

    for game in gog_catalog:
        url_name = game["url_name"]
        if url_name in url_names:
            coincidences.append({
                'url': game["url"],
                'url_name': url_name,
                'price_in_cents': None,
                'price_time': None
            })

    write_json(os.path.join("temp", "gog_coincidences.json"), coincidences)
    logger('INFO', f'{len(coincidences)} coincidences found')

    logger('INFO', 'Started crawling gog.com prices')
    try:
        run_crawler('gog')
    except:
        logger('ERROR', traceback.format_exc())
    logger('INFO', 'Ended crawling gog.com prices')

    logger('INFO', 'Started updating gog.com prices')

    gog_coincidences = read_json(os.path.join("temp", "gog_coincidences.json"))
    gog_coincidences_dict = {coincidence["url_name"]: coincidence for coincidence in gog_coincidences}

    for game in games:
        if game["url_name"] in gog_coincidences_dict:
            if gog_coincidences_dict[game["url_name"]]["price_in_cents"] != None:
                game["stores"]["gog"]["availability"] = True
                game["stores"]["gog"]["price_in_cents"] = gog_coincidences_dict[game["url_name"]]["price_in_cents"]
                game["stores"]["gog"]["price_time"] = gog_coincidences_dict[game["url_name"]]["price_time"]
                game["stores"]["gog"]["url"] = gog_coincidences_dict[game["url_name"]]["url"]
                # Prices history (gog)
                if game["stores"]["gog"]["price_in_cents"] is not None and game["stores"]["gog"]["price_in_cents"] >= 0:
                    if game["appid"] in prices_history_dict:
                        prices_history_dict[game["appid"]]["gog"].append({
                            "price_in_cents": game["stores"]["gog"]["price_in_cents"],
                            "price_time": game["stores"]["gog"]["price_time"],
                        })
            else:
                game["stores"]["gog"]["availability"] = False
                game["stores"]["gog"]["price_in_cents"] = None
                game["stores"]["gog"]["price_time"] = get_time()
                game["stores"]["gog"]["url"] = None
        else:
            game["stores"]["gog"]["availability"] = False
            game["stores"]["gog"]["price_in_cents"] = None
            game["stores"]["gog"]["price_time"] = get_time()
            game["stores"]["gog"]["url"] = None

    if len(gog_coincidences_dict) > 0:
        write_json('games.json', games)
        write_json('prices_history.json', list(prices_history_dict.values()))

    logger('INFO', 'Ended updating gog.com prices')

def json_to_ndjson(input_filename, output_filename):
    data = read_json(input_filename)
    file_path = os.path.join(parent_path, 'ndjson_data', output_filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            appid = item.get("appid")
            if appid is not None:
                meta_line = json.dumps({ "create": { "_id": appid } })
                doc_line = json.dumps(item, ensure_ascii=False)
                f.write(meta_line + "\n")
                f.write(doc_line + "\n")

        logger(f'INFO', f'Formatted JSON file {input_filename} to NDJSON {output_filename}.')

def json_list_to_ndjson(input_filename, output_filename):
    data = read_json(input_filename)
    formatted_data = [{"name": g} for g in data]
    file_path = os.path.join(parent_path, 'ndjson_data', output_filename)
    i = 0
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in formatted_data:
            meta_line = json.dumps({"create": {"_id": i}})
            doc_line = json.dumps(item, ensure_ascii=False)
            f.write(meta_line + "\n")
            f.write(doc_line + "\n")
            i+=1

        logger(f'INFO', f'Formatted JSON file {input_filename} to NDJSON {output_filename}.')

if __name__ == '__main__':
    try:
        initialize()
        #fetch_steam_catalog()
        fetch_steam_catalog_by_ids([10, 311210, 1174180, 377160, 552520, 2344520, 1985820, 1091500, 214490, 1002300, 1245620, 646270, 235600, 1888930, 1716740, 268910, 3180070, 1716740, 668580, 202970, 235600, 1771300, 1085660, 2767030, 578080, 1962663, 1665460, 440, 570]) # TEST
        fetch_steam_details()
        fetch_epic_catalog()
        fetch_battle_catalog()
        fetch_xbox_catalog()
        fetch_gog_catalog()
        json_to_ndjson("games.json", "games_bulk.ndjson")
        json_list_to_ndjson("categories.json", "categories_bulk.ndjson")
        json_list_to_ndjson("genres.json", "genres_bulk.ndjson")
        json_list_to_ndjson("developers.json", "developers_bulk.ndjson")
        json_list_to_ndjson("publishers.json", "publishers_bulk.ndjson")
        json_list_to_ndjson("pegi.json", "pegi_bulk.ndjson")
        json_to_ndjson("prices_history.json", "prices_history_bulk.ndjson")
        finalize()

    except:
        finalize(traceback)
        logger('ERROR', traceback.format_exc())