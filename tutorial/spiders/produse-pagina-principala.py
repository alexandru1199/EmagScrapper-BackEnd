import scrapy
import json
import os
import requests
from datetime import datetime
from urllib.parse import urlparse
from collections import defaultdict
import pytz
from emag_cart_uils import run_get_max_quantities
from concurrent.futures import ThreadPoolExecutor, as_completed

class ProduseSpider(scrapy.Spider):
    name = "produse-pagina-principala"
    custom_settings = {
        "FEED_EXPORT_ENCODING": "utf-8",
        "DOWNLOAD_DELAY": 0.5,
        "LOG_LEVEL": "DEBUG",
    }

    def __init__(self, categories_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = 5
        self.raw_produse = defaultdict(list)
        self.output_path = "output/produse-pagina-principala.json"
        self.session = self.init_emag_session()
        self.index_existing = self.fetch_existing_indices()
        self.output_produse = []

        # ✅ Mereu setează același nume: self.categories_path
        if categories_file:
            self.categories_path = os.path.abspath(categories_file)
        else:
            self.categories_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "categorii_active.json")
            )
    def init_emag_session(self):
        s = requests.Session()
        s.headers.update({
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.emag.ro/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
        })
        try:
            s.get("https://www.emag.ro/")
        except Exception as e:
            self.logger.warning(f"[WARNING] Nu s-a putut inițializa sesiunea eMAG: {e}")
        return s

    def fetch_existing_indices(self):
        try:
            r = requests.get("http://localhost:5000/produse")
            if r.status_code != 200:
                self.logger.warning("[WARNING] Eroare la preluare produse existente")
                return {}
            data = r.json()
            return {
                str(p["ID"]).strip(): {
                    "Index": p["Index"],
                    "Page": p["Page"],
                    "Categorie": p["Categorie"],
                    "RawPosition": p.get("RawPosition")
                } for p in data
            }
        except Exception as e:
            self.logger.warning(f"[WARNING] Eroare fetch existing index: {e}")
            return {}

    def start_requests(self):
        # 🔹 Încarcă direct din fișierul JSON categoriile active
        with open(self.categories_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            categorii_active = set(data.get("active", []))

        # 🔹 Construiește URL-uri paginare pentru fiecare categorie activă
        for categorie in categorii_active:
            categorie = categorie.strip().rstrip("/")
            if not categorie:
                continue

            for page in range(1, self.max_pages + 1):
                url = f"https://www.emag.ro/{categorie}/c" if page == 1 else f"https://www.emag.ro/{categorie}/p{page}/c"

                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={'categorie': categorie, 'page': page}
                )

    def extract_categorie(self, url):
        path = urlparse(url).path
        parts = path.strip("/").split("/")
        return parts[0] if parts else "necunoscuta"

    def parse(self, response):
        categorie = response.meta['categorie']
        page = response.meta['page']
        self.logger.info(f"[INFO] Pagina {page} - Categorie: {categorie} - URL: {response.url}")

        products = response.css("div.card-item.js-product-data")
        if not products:
            self.logger.warning(f"[WARNING] Niciun produs găsit pe pagina {page} - {response.url}")
            return

        self.logger.info(f"[DEBUG] Găsite {len(products)} produse brute pe pagina {page}")

        seen_ids = {p["ProductID"] for p in self.raw_produse[categorie]}

        for product in products:
            # Preț brut din HTML
            price_text = product.css("p.product-new-price::text").get()
            if not price_text:
                continue

            # Curățare și conversie
            price_clean = price_text.replace(".", "").replace(" Lei", "").replace(",", "").strip()
            try:
                price_ron = float(price_clean)
            except ValueError:
                continue
            if not (50 <= price_ron <= 1000):
                continue

            # Review-uri (fallback între .hidden-xs și .visible-xs-inline-block)
            review_text = (
                    product.css("span.hidden-xs::text").get() or
                    product.css("span.visible-xs-inline-block::text").get()
            )
            import re
            match = re.search(r'\d+', review_text or "")
            try:
                review_count = int(match.group(0)) if match else 0
            except ValueError:
                review_count = 0

            if review_count > 20:
                continue

            # Alte atribute
            name = product.attrib.get("data-name")
            product_id = product.attrib.get("data-product-id")
            offer_id = product.attrib.get("data-offer-id")
            image = product.css("img::attr(src)").get()
            product_url = product.css("a::attr(href)").get()
            data_position = product.attrib.get("data-position")

            if not name or not product_id or not offer_id or product_id in seen_ids:
                continue

            try:
                raw_position = int(data_position)
            except (TypeError, ValueError):
                raw_position = 99999

            if product_url:
                product_url = response.urljoin(product_url.strip())

            self.raw_produse[categorie].append({
                "ProductID": product_id.strip(),
                "OfferID": offer_id.strip(),
                "ProductName": name.strip(),
                "Image": image,
                "Categorie": categorie,
                "CategorieURL": response.url,
                "Page": page,
                "RawPosition": raw_position,
                "Stock": None,
                "ProductURL": product_url,
                "Price": price_ron,
                "ReviewCount": review_count
            })

    def closed(self, reason):
        romania_tz = pytz.timezone("Europe/Bucharest")
        now_local = datetime.now(romania_tz)

        produse_de_actualizat = []

        for categorie, produse in self.raw_produse.items():
            produse.sort(key=lambda p: (p["Page"], p.get("RawPosition", 99999)))

            for idx, produs in enumerate(produse, start=1):
                produs.update({
                    "Index": idx,
                    "TimeStamp": now_local.isoformat(),
                    "ID": produs["ProductID"]
                })

                produs_id = produs["ID"]
                poz_veche = self.index_existing.get(produs_id, {})
                poz_noua = (produs["Page"], produs["Index"])
                poz_veche_tuple = (poz_veche.get("Page"), poz_veche.get("Index"))
                raw_vechi = poz_veche.get("RawPosition")
                raw_nou = produs.get("RawPosition")

                produs_nou = not poz_veche
                poz_mutata = poz_veche_tuple != poz_noua
                raw_mutat = raw_vechi != raw_nou

                if produs_nou or poz_mutata or raw_mutat:
                    self.logger.info(
                        f"[DEBUG] PRODUS ACTUALIZAT: {produs_id} | "
                        f"nou={produs_nou}, poz_mutata={poz_mutata}, raw_mutat={raw_mutat} | "
                        f"poz_veche={poz_veche_tuple}, poz_noua={poz_noua}, raw_vechi={raw_vechi}, raw_nou={raw_nou}"
                    )
                    produse_de_actualizat.append({
                        "offer_id": produs["OfferID"],
                        "categorieURL": produs["CategorieURL"],
                        "product_ref": produs
                    })

        if produse_de_actualizat:
            self.logger.info(f"[STOCK] {len(produse_de_actualizat)} produse noi sau mutate – preluare stock...")

            toate_requesturile = [
                {
                    "offer_id": p["offer_id"],
                    "categorieURL": p["categorieURL"],
                    "product_ref": p["product_ref"]
                }
                for p in produse_de_actualizat
            ]
            batch_size = 60
            stock_map = {}

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(run_get_max_quantities, toate_requesturile[i:i + batch_size])
                    for i in range(0, len(toate_requesturile), batch_size)
                ]
                for future in as_completed(futures):
                    rezultate = future.result()
                    for r in rezultate:
                        stock_map[r["offer_id"]] = r["max_quantity"]

            # Transformă valorile "necunoscut" în -1
            for offer_id, stock in stock_map.items():
                if stock == "necunoscut":
                    stock_map[offer_id] = -1

            # Actualizează stocul în produsele finale
            for p in produse_de_actualizat:
                produs = p["product_ref"]
                offer_id = produs["OfferID"]
                old_stock = self.index_existing.get(produs["ID"], {}).get("Stock", None)
                new_stock = stock_map.get(offer_id, -1)

                produs["OldStock"] = old_stock
                produs["NewStock"] = new_stock
                produs["Stock"] = new_stock

            # Trimite doar cele cu stoc valid
            self.send_batches([
                p["product_ref"]
                for p in produse_de_actualizat
                if p["product_ref"].get("Stock", -1) != -1
            ])

            print(
                f"[INFO] Export completat: {len(produse_de_actualizat)} produse actualizate și salvate în {self.output_path}")
        else:
            print("[INFO] Niciun produs nou sau mutat pentru actualizare.")

        print(f"[INFO] Export completat: {len(self.output_produse)} produse salvate în {self.output_path}")
    def send_batches(self, produse):
        batch_size = 1000
        for i in range(0, len(produse), batch_size):
            batch = produse[i:i + batch_size]
            try:
                r = requests.post("http://localhost:5000/procesare-json-bulk", json=batch)
                if r.status_code != 200:
                    self.logger.warning(f"[WARNING] Eșec POST batch: {r.status_code} - {r.text}")
            except Exception as e:
                self.logger.warning(f"[WARNING] POST batch error: {e}")
