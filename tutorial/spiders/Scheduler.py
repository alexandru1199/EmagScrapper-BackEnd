import schedule
import subprocess
import time
import datetime
import os
import json
from threading import Thread

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

def run_spider_with_categories(categories, part_index):
    filename = f"categorii_part_{part_index}.json"
    filepath = os.path.abspath(filename)

    # Construiește JSON-ul activ cu lista de categorii
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump({"active": categories}, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Rulez spiderul pentru partitia {part_index} cu {len(categories)} categorii...")

    subprocess.run([
        "scrapy", "crawl", "produse-pagina-principala",
        "-a", f"categories_file={filepath}"
    ])
    print(f"[INFO] Spider partitia {part_index} s-a terminat.")

def run_parallel_spiders():
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[INFO] Rulez spider-ele în paralel la {timestamp}...")

    # Citește categoriile active din JSON
    with open("categorii_active.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        all_categories = data.get("active", [])

    if not all_categories:
        print("[WARN] Nicio categorie activă găsită în JSON.")
        return

    num_parts = 2  # ← modifică aici câte instanțe rulezi în paralel
    parts = split_list(all_categories, num_parts)

    threads = []
    for i, part in enumerate(parts):
        t = Thread(target=run_spider_with_categories, args=(part, i + 1))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("[INFO] Toate spider-ele s-au terminat.")

if __name__ == "__main__":
    run_parallel_spiders()
    schedule.every(480).minutes.do(run_parallel_spiders)
    print("[INFO] Scheduler pornit — spider-ele vor rula în paralel la fiecare 8 ore.")
    while True:
        schedule.run_pending()
        time.sleep(1)
