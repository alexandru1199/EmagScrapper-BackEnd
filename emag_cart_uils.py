import time
import json
import logging
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
import os

MAX_CART_SIZE = 50
CAPTCHA_MAX_RETRIES = 5

logger = logging.getLogger("EmagCartBot")
logging.getLogger().setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

def init_browser_and_session(start_url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
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
            fix_hairline=True)

    driver.get(start_url)

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

def detect_captcha(driver):
    try:
        html = driver.page_source.lower()
        if "am detectat trafic neobișnuit" in html or "am detectat trafic neobisnuit" in html:
            return True
        if "cloudflare" in html and "verifying" in html:
            return True
        if "g-recaptcha" in html or "grecaptcha" in html:
            return True
        return False
    except Exception:
        return False

def get_max_quantities_from_emag(products, driver, session):
    os.makedirs("output", exist_ok=True)
    rezultate = []
    skip_log = []
    retry_queue = []

    driver.execute_script("window.open('about:blank','_blank');")
    cart_tab_handle = driver.window_handles[-1]

    index = 0
    processed_in_current_session = 0

    while index < len(products):
        p = products[index]
        offer_id = p["offer_id"]
        produs = p["product_ref"]
        product_url = produs.get("ProductURL", "").strip()

        for captcha_attempt in range(CAPTCHA_MAX_RETRIES):
            try:
                driver.switch_to.window(driver.window_handles[0])
                driver.get(product_url)
                time.sleep(0.5)

                if detect_captcha(driver):
                    logger.warning(f"[CAPTCHA RETRY] {offer_id}: încercarea {captcha_attempt + 1}/{CAPTCHA_MAX_RETRIES}")
                    if captcha_attempt + 1 >= CAPTCHA_MAX_RETRIES:
                        logger.warning(f"[CAPTCHA FAIL] {offer_id} eșuat după {CAPTCHA_MAX_RETRIES} încercări.")
                        skip_log.append({"offer_id": offer_id, "url": product_url, "reason": "captcha multiple fail"})
                        break
                    driver.quit()
                    driver, session = init_browser_and_session("https://www.emag.ro/")
                    driver.execute_script("window.open('about:blank','_blank');")
                    cart_tab_handle = driver.window_handles[-1]
                    processed_in_current_session = 0
                    continue

                html = driver.page_source.lower()
                if "vezi ofertele" in html:
                    logger.warning(f"[SKIP] {offer_id} – produs cu oferte multiple.")
                    skip_log.append({"offer_id": offer_id, "url": product_url, "reason": "oferte multiple"})
                    break

                selector = f"button.yeahIWantThisProduct[data-offer-id='{offer_id}']"
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                if not buttons:
                    alt_buttons = driver.find_elements(By.CSS_SELECTOR, "a.js-unfair-offer-btn")
                    if alt_buttons:
                        logger.warning(f"[SKIP] {offer_id} – produs cu oferte multiple.")
                        skip_log.append({"offer_id": offer_id, "url": product_url, "reason": "oferte multiple"})
                        break
                    raise Exception("buton lipsă")

                button = buttons[0]
                driver.execute_script("arguments[0].click();", button)
                time.sleep(0.3)

                driver.switch_to.window(cart_tab_handle)
                driver.get("https://www.emag.ro/shopping/header-cart?act=load&source=front")
                time.sleep(0.3)

                raw_text = driver.execute_script("return document.body.innerText;")
                cart_data = json.loads(raw_text)
                total_products = cart_data.get("total_products", 0)
                if total_products >= MAX_CART_SIZE:
                    logger.warning(f"[QUEUE] {offer_id} – coș plin, produs salvat pentru sesiune nouă")

                    if p not in retry_queue:
                        retry_queue.append(p)

                    driver.quit()
                    driver, session = init_browser_and_session("https://www.emag.ro/")
                    driver.execute_script("window.open('about:blank','_blank');")
                    cart_tab_handle = driver.window_handles[-1]
                    processed_in_current_session = 0

                    break

                lines = cart_data.get("lines", [])
                found = False
                max_q = -1

                for line in lines:
                    if str(line.get("id")) != offer_id:
                        continue
                    line_main = line.get("line_main", {})
                    max_q = line_main.get("max_quantity", -1)
                    if max_q in (-1, None):
                        fbo = line_main.get("formatted_buying_options")
                        if isinstance(fbo, str):
                            try:
                                max_q = json.loads(fbo).get("max", -1)
                            except:
                                pass
                    found = True
                    break

                if not found:
                    logger.warning(f"[SKIP] {offer_id} – not found în coș")
                    skip_log.append({"offer_id": offer_id, "url": product_url, "reason": "not found în coș"})
                    break

                rezultat = {
                    "offer_id": offer_id,
                    "max_quantity": max_q
                }
                rezultate.append(rezultat)
                with open("output/rezultate_partial.json", "a", encoding="utf-8") as f:
                    f.write(json.dumps(rezultat, ensure_ascii=False) + "\n")

                processed_in_current_session += 1
                if processed_in_current_session >= MAX_CART_SIZE:
                    driver.quit()
                    driver, session = init_browser_and_session("https://www.emag.ro/")
                    driver.execute_script("window.open('about:blank','_blank');")
                    cart_tab_handle = driver.window_handles[-1]
                    processed_in_current_session = 0

                break

            except Exception as e:
                logger.warning(f"[SKIP] {offer_id} – {e}")
                skip_log.append({"offer_id": offer_id, "url": product_url, "reason": str(e)})
                break

        index += 1

        if index >= len(products) and retry_queue:
            products = retry_queue
            retry_queue = []
            index = 0
            driver.quit()
            driver, session = init_browser_and_session("https://www.emag.ro/")
            driver.execute_script("window.open('about:blank','_blank');")
            cart_tab_handle = driver.window_handles[-1]
            processed_in_current_session = 0

    if skip_log:
        with open("output/produse_skipate.json", "w", encoding="utf-8") as f:
            json.dump(skip_log, f, ensure_ascii=False, indent=2)

    try:
        driver.quit()
    except:
        pass

    return rezultate


def run_get_max_quantities(batch):
    try:
        driver, session = init_browser_and_session("https://www.emag.ro/")
        return get_max_quantities_from_emag(batch, driver, session)
    except Exception as e:
        logger.error(f"[FATAL] Eșec sesiune: {e}")
        return []
