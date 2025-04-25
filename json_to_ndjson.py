from datetime import datetime
import json
import os

parent_path = './' # Default
#parent_path = '/home/raspy/Desktop/theEasterEgg_scraper/' # Crontab

def logger(status, message, html_code=None):
    if html_code:
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{status}|{html_code}|{message}")
    else:
        print(f"|{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|{status}|{message}")

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

def write_ndjson(data, output_filename):
    file_path = os.path.join(parent_path, 'json_data', output_filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            appid = item.get("appid")
            if appid is not None:
                meta_line = json.dumps({ "create": { "_id": appid } })
                doc_line = json.dumps(item, ensure_ascii=False)
                f.write(meta_line + "\n")
                f.write(doc_line + "\n")

        logger(f'INFO', f'Formatted JSON file to NDJSON.')


if __name__ == '__main__':
    data = read_json("games.json")
    write_ndjson(data, "games_bulk.ndjson")