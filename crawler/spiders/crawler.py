import json
import os.path
import re
import traceback

from scrapy.spiders import Spider
from main import logger, get_time

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

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

def get_xbox_price(price_span):
    price_span = price_span.lower().strip()

    if "gratis" in price_span:
        return 0

    # Formatos "9,99 €", "9,99€", "9,99€+"
    match = re.search(r"(\d+),(\d{2})\s*€\+?", price_span)
    if match:
        euros = int(match.group(1))
        cents = int(match.group(2))
        return euros * 100 + cents

    return -1

def get_battle_prices(self, data):
    """ Función recursiva para extraer todas las claves 'price' de un JSON anidado """
    prices = []

    if isinstance(data, dict):
        for key, value in data.items():
            if key == "price":
                prices.append(value)
            else:
                prices.extend(get_battle_prices(self, value))

    elif isinstance(data, list):
        for item in data:
            prices.extend(get_battle_prices(self, item))

    return prices

def battle_prices_list_string_to_list_int(prices):
    return [int(float(price) * 100) for price in prices]

class CrawlerSpider(Spider):
    name = "no-scraper"
    allowed_domains = [
        "www.xbox.com/",
        "eu.shop.battle.net/",
        "www.gog.com/"
    ]

    def __init__(self, mode, *args, **kwargs):
        super().__init__(*args, **kwargs)
        urls = []
        match mode:
            case "xbox":
                coincidences = read_json(os.path.join("temp", "xbox_coincidences.json"))
                for coincidence in coincidences:
                    urls.append(coincidence["url"])
                self.coincidences_dict = {coincidence["url_name"]: coincidence for coincidence in coincidences}

            case "battle":
                coincidences = read_json(os.path.join("temp", "battle_coincidences.json"))
                for coincidence in coincidences:
                    urls.append(coincidence["url"])
                self.coincidences_dict = {coincidence["url_name"]: coincidence for coincidence in coincidences}

            case "gog":
                coincidences = read_json(os.path.join("temp", "gog_coincidences.json"))
                for coincidence in coincidences:
                    urls.append(coincidence["url"])
                self.coincidences_dict = {coincidence["url_name"]: coincidence for coincidence in coincidences}

            case _:
                logger('ERROR', 'Crawler mode not recognized')

        self.start_urls = urls
        self.mode = mode

    def parse(self, response):
        try:
            match self.mode:
                case "xbox":
                    try:
                        price_span = response.css("div.ProductDetailsHeader-module__price___-NaHV span.Price-module__boldText___1i2Li::text").get()

                        if price_span:
                            current_price_in_cents = get_xbox_price(price_span)
                        else:
                            game_pass_label = response.css("div.ProductLogos-module__gamePassLogo___UxbvF svg::attr(aria-label)").get()
                            if game_pass_label and "incluido con game pass" in game_pass_label.lower().strip():
                                current_price_in_cents = -2
                            else:
                                current_price_in_cents = -1

                        current_url_name = response.url.split("store/")[1].split("/")[0]
                        if current_url_name in self.coincidences_dict:
                            self.coincidences_dict[current_url_name]["price_in_cents"] = current_price_in_cents
                            self.coincidences_dict[current_url_name]["url"] = response.url
                            self.coincidences_dict[current_url_name]["price_time"] = get_time()

                    except json.JSONDecodeError as e:
                        logger('ERROR', e)

                case "battle":
                    try:
                        game_content = response.xpath('//script[@class="structured-product-data"]/text()').get()

                        if game_content:
                            data = json.loads(game_content)
                            prices_string = get_battle_prices(self, data)
                            prices_int = battle_prices_list_string_to_list_int(prices_string)
                            current_price_in_cents = min(prices_int)
                        else:
                            current_price_in_cents = -1

                        current_url_name = response.url.split("product/")[1]
                        if current_url_name in self.coincidences_dict:
                            self.coincidences_dict[current_url_name]["price_in_cents"] = current_price_in_cents
                            self.coincidences_dict[current_url_name]["url"] = response.url
                            self.coincidences_dict[current_url_name]["price_time"] = get_time()

                    except json.JSONDecodeError as e:
                        logger('ERROR', e)

                case "gog":
                    try:
                        if response.url != 'https://www.gog.com/en/games':
                            price_in_cents = -1
                            json_ld_script = response.xpath('//script[@type="application/ld+json"]/text()').get()
                            if json_ld_script:
                                data = json.loads(json_ld_script)
                                offers = data.get("offers", [])

                                if isinstance(offers, list):
                                    for offer in offers:
                                        if offer.get("areaServed") == "ES" and offer.get("priceCurrency") == "EUR":
                                            price_in_cents = int(float(offer.get("price")) * 100)
                                            break

                            current_url_name = response.url.split("game/")[1].replace("_", "-")
                            if current_url_name in self.coincidences_dict:
                                self.coincidences_dict[current_url_name]["price_in_cents"] = price_in_cents
                                self.coincidences_dict[current_url_name]["url"] = response.url
                                self.coincidences_dict[current_url_name]["price_time"] = get_time()

                    except json.JSONDecodeError as e:
                        logger('ERROR', e)

                case _:
                    logger('ERROR', 'Crawler mode not recognized')
        except:
            logger('ERROR', traceback.format_exc())

    def closed(self, reason):
        match self.mode:
            case "xbox":
                logger('INFO', "Started updating JSON file 'xbox_coincidences.json'")
                write_json(os.path.join("temp", "xbox_coincidences.json"), list(self.coincidences_dict.values()))
                logger('INFO', "Ended updating JSON file 'xbox_coincidences.json'")

            case "battle":
                logger('INFO', "Started updating JSON file 'battle_coincidences.json'")
                write_json(os.path.join("temp", "battle_coincidences.json"), list(self.coincidences_dict.values()))
                logger('INFO', "Ended updating JSON file 'battle_coincidences.json'")

            case "gog":
                logger('INFO', "Started updating JSON file 'gog_coincidences.json'")
                write_json(os.path.join("temp", "gog_coincidences.json"), list(self.coincidences_dict.values()))
                logger('INFO', "Ended updating JSON file 'gog_coincidences.json'")

            case _:
                logger('ERROR', 'Crawler mode not recognized')