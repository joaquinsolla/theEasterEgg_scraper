from scrapy.spiders import Spider
from basics import get_json_content

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

class CrawlerSpider(Spider):
    name = "no-scraper"
    allowed_domains = [
        "store.steampowered.com/"
    ]

    def __init__(self, mode, *args, **kwargs):
        super().__init__(*args, **kwargs)
        urls = []
        match mode:
            case "steam":
                data = get_json_content(f"{parent_path}json_data/steam_apps.json")
                for item in data:
                    if len(item["name"]) > 0:
                        urls.append("https://store.steampowered.com/app/" + str(item["appid"]))

            case _:
                print("Modo no reconocido")
        self.start_urls = urls
        self.mode = mode

    def parse(self, response):
        match self.mode:
            case "steam":
                if response.url != "https://store.steampowered.com/":
                    print(" - " + response.url)
                    print(response.text)



                    "https://chatgpt.com/share/67a8b681-b0b8-8007-8ad8-96bdbd5b8339"