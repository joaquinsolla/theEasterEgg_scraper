from scrapy.spiders import Spider
from main import read_json, write_json, logger

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

class CrawlerSpider(Spider):
    name = "no-scraper"
    allowed_domains = [
        "www.ea.com/",
        "www.xbox.com/",
        "eu.shop.battle.net/",
        # TODO: Falta Rockstar
    ]

    def __init__(self, mode, *args, **kwargs):
        super().__init__(*args, **kwargs)
        urls = []
        match mode:
            case "x":

                urls.append("")

            case _:
                print("Modo no reconocido")
        self.start_urls = urls
        self.mode = mode

    def parse(self, response):
        match self.mode:
            case "x":
                print()