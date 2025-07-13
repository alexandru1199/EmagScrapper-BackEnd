import time
import random
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import time
import logging
from selenium.webdriver.common.by import By
logger = logging.getLogger(__name__)

MAX_PER_BATCH = 50
def detect_captcha_in_cart_response(raw_text):
    """Verifică dacă răspunsul din pagina de coș pare a fi CAPTCHA."""
    keywords = ["captcha", "robot", "checkbox", "dovedește că nu ești"]
    return any(k in raw_text.lower() for k in keywords)
def init_browser_and_session(start_url):
    chrome_options = Options()

    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=chrome_options)

    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    driver.get(start_url)
    time.sleep(random.uniform(1.0, 2.0))

    session = requests.Session()
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])

    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": start_url
    })

    return driver, session

def maybe_bypass_captcha(driver):
    try:
        checkbox = driver.find_element(By.CSS_SELECTOR, 'input[type="checkbox"]')
        driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(2)
    except:
        pass


def scroll_and_click_products(driver, produse, max_scrolls=15, scroll_pause=1.5):
    import time
    from selenium.webdriver.common.by import By
    import logging

    logger = logging.getLogger(__name__)
    not_found = {p["offer_id"]: p for p in produse}
    found = set()

    scrolls = 0
    retry_captcha = 0

    while not_found and scrolls < max_scrolls:
        logger.debug(f"[SCROLL] Scroll #{scrolls + 1} — Rămase: {len(not_found)}")
        found_in_this_scroll = set()

        for offer_id, produs in list(not_found.items()):
            try:
                btn = driver.find_element(By.CSS_SELECTOR, f'button.yeahIWantThisProduct[data-offer-id="{offer_id}"]')
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", btn)
                logger.debug(f"[CLICK] Click pe produs {offer_id}")
                found_in_this_scroll.add(offer_id)
                found.add(offer_id)
            except Exception as e:
                logger.debug(f"[MISS] Butonul pt {offer_id} nu a fost găsit")

        for oid in found_in_this_scroll:
            not_found.pop(oid, None)

        if not found_in_this_scroll:
            # Nu s-a găsit nimic nou — printăm ce e în pagină
            all_btns = driver.find_elements(By.CSS_SELECTOR, 'button.yeahIWantThisProduct[data-offer-id]')
            visible_offer_ids = [btn.get_attribute("data-offer-id") for btn in all_btns if btn.get_attribute("data-offer-id")]
            logger.debug(f"[DEBUG] Butoane vizibile pe pagină: {visible_offer_ids}")

            if len(visible_offer_ids) < 2:
                retry_captcha += 1
                if retry_captcha <= 5:
                    logger.debug("[CAPTCHA] Posibil captcha (puține butoane vizibile), reîncercare pagină...")
                    driver.refresh()
                    time.sleep(4)
                    continue
                else:
                    logger.warning("[CAPTCHA] Prea multe încercări de bypass captcha, renunț.")
                    break

        scrolls += 1
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause)

    logger.debug(f"[DONE] Gata scrollarea — găsite {len(found)} / {len(produse)} produse")


def fetch_cart_data(session, driver=None, retries=10):
    for attempt in range(retries):
        try:
            if driver:
                raw_text = driver.execute_script("return document.body.innerText;").strip()
                if detect_captcha_in_cart_response(raw_text):
                    print("[CAPTCHA] Detectat în răspunsul de coș. Așteptăm și retry...")
                    time.sleep(10)
                    continue
                if not raw_text:
                    raise Exception("Răspunsul de coș este gol.")
                return json.loads(raw_text)
            else:
                response = session.get("https://www.emag.ro/shopping/header-cart?act=load&source=front")
                if response.status_code == 200:
                    raw_text = response.text.strip()
                    if detect_captcha_in_cart_response(raw_text):
                        print("[CAPTCHA] Detectat (requests). Așteptăm și retry...")
                        time.sleep(10)
                        continue
                    return response.json()
        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON invalid — {e}")
        except Exception as e:
            print(f"[ERROR] La fetch cart: {e}")
        time.sleep(1)
    raise Exception("GET cart eșuat după multiple încercări.")

def extract_max_quantities(cart_json):
    result = []
    for line in cart_json.get("lines", []):
        try:
            formatted = line.get("line_main", {}).get("formatted_buying_options", "{}")
            buying_options = json.loads(formatted)
            max_q = buying_options.get("max", "necunoscut")
            result.append({
                "offer_id": line.get("id"),  # <- AICI E IDENTIFICATORUL REAL FOLOSIT ÎN COȘ
                "name": line.get("name"),
                "max_quantity": max_q
            })
        except Exception:
            continue
    return result


def get_max_quantities_from_emag(driver, cart_tab_handle):
    import json, logging, time
    driver.switch_to.window(cart_tab_handle)
    driver.get("https://www.emag.ro/shopping/header-cart?act=load&source=front")
    time.sleep(1)
    rezultate = []
    try:
        cart_data = json.loads(driver.execute_script("return document.body.innerText;"))
        for line in cart_data.get("lines", []):
            try:
                offer_id = str(line.get("id"))
                max_q = line.get("line_main", {}).get("max_quantity", -1)
                rezultate.append({
                    "offer_id": offer_id,
                    "max_quantity": max_q if isinstance(max_q, int) else -1
                })
            except Exception as e:
                logging.warning(f"[MISS] Eroare la parsare produs în coș: {e}")
    except Exception as e:
        logging.error(f"[FAIL] Nu s-a putut prelua coșul: {e}")
    return rezultate
