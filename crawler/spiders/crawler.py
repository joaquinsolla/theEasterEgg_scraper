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

class CrawlerSpider(Spider):
    name = "no-scraper"
    allowed_domains = [
        "www.xbox.com/",
        "eu.shop.battle.net/",
        # TODO: Falta Rockstar
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

            case _:
                logger('ERROR', 'Crawler mode not recognized')

        self.start_urls = urls
        self.mode = mode

    def parse(self, response):
        try:
            match self.mode:
                case "xbox":
                    price_span = response.css("div.ProductDetailsHeader-module__price___-NaHV span.Price-module__boldText___1i2Li::text").get()

                    if price_span:
                        current_price_in_cents = get_xbox_price(price_span)
                    else:
                        game_pass_label = response.css("div.ProductLogos-module__gamePassLogo___UxbvF svg::attr(aria-label)").get()
                        if game_pass_label and "incluido con game pass" in game_pass_label.lower().strip():
                            current_price_in_cents = -2
                        else:
                            current_price_in_cents = -1

                    current_price_time = get_time()
                    current_url_name = response.url.split("store/")[1].split("/")[0]

                    if current_url_name in self.coincidences_dict:
                        self.coincidences_dict[current_url_name]["price_in_cents"] = current_price_in_cents
                        self.coincidences_dict[current_url_name]["price_time"] = current_price_time

                case _:
                    logger('ERROR', 'Crawler mode not recognized')
        except:
            logger('ERROR', traceback.format_exc())

    def closed(self, reason):
        logger('INFO', "Started updating JSON file 'xbox_coincidences.json'")
        write_json(os.path.join("temp", "xbox_coincidences.json"), list(self.coincidences_dict.values()))
        logger('INFO', "Ended updating JSON file 'xbox_coincidences.json'")