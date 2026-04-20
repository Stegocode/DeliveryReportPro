# -*- coding: utf-8 -*-
"""
financial_scraper_app.py
========================
DeliveryReportPro scraper — wraps the Homesource Selenium automation
for use inside the app pipeline.
Credentials come from os.environ (injected by pipeline.py from encrypted config).
Progress is emitted via the ProgressEmitter/ErrorRegistry from pipeline.py.
ChromeDriver is managed by chromedriver_manager.py — no PATH required.
"""

import time
import os
import requests as req_lib
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# Credentials from environment (set by pipeline.py, cleared after run)
HS_USERNAME = os.getenv("HS_USERNAME", "")
HS_PASSWORD = os.getenv("HS_PASSWORD", "")

# ── Homesource instance configuration ──────────────────────────────
# Each dealer has their own Homesource subdomain. Edit this to match
# your instance, or override via HS_BASE_URL env var.
# Example: https://acme1.homesourcesystems.com
BASE_URL = os.getenv("HS_BASE_URL", "https://your-subdomain.homesourcesystems.com")

# Inbox lives in APPDATA so no admin rights needed
INBOX_DIR = os.path.join(
    os.environ.get("APPDATA", os.path.expanduser("~")),
    "DeliveryReportPro", "scrape_inbox"
)
os.makedirs(INBOX_DIR, exist_ok=True)



# ── Chrome setup ───────────────────────────────────────────────────

def make_driver(driver_path: str):
    """
    Creates a headless Chrome WebDriver using the provided ChromeDriver path.
    Downloads go directly to INBOX_DIR — no PATH, no admin rights.

    Args:
        driver_path: absolute path to chromedriver.exe from chromedriver_manager

    Error code: DRIVER_LAUNCH_FAIL
    """
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option('prefs', {
        'download.default_directory':   INBOX_DIR,
        'download.prompt_for_download': False,
        'download.directory_upgrade':   True,
        'safebrowsing.enabled':         True,
    })
    service = Service(executable_path=driver_path)
    return webdriver.Chrome(service=service, options=options)


def js_click(driver, el):
    driver.execute_script("arguments[0].click();", el)


def wait_for_download(keyword, timeout=45, emitter=None):
    if emitter: emitter.log(f'Waiting for download: {keyword}…')
    for _ in range(timeout):
        matches = [
            f for f in os.listdir(INBOX_DIR)
            if keyword.lower() in f.lower() and not f.endswith('.crdownload')
        ]
        if matches:
            path = os.path.join(INBOX_DIR, matches[0])
            if emitter: emitter.log(f'Downloaded: {matches[0]}', 'good')
            return path
        time.sleep(1)
    if emitter: emitter.log(f'Timed out waiting for: {keyword}', 'warn')
    return None


def get_requests_session(driver):
    """Copy Selenium cookies into a requests.Session for direct HTTP calls."""
    session = req_lib.Session()
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})
    return session


# ── Login ──────────────────────────────────────────────────────────

def login(driver, wait, emitter, errors):
    emitter.running('login', 'Signing in to HomeSource')
    driver.get(f"{BASE_URL}/login")
    time.sleep(6)
    try:
        wait.until(EC.presence_of_element_located((By.NAME, 'email')))
        driver.find_element(By.NAME, 'email').send_keys(HS_USERNAME)
        driver.find_element(By.NAME, 'password').send_keys(HS_PASSWORD)
        driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(4)
        try:
            save_btn = WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, "save-current-location"))
            )
            time.sleep(1)
            js_click(driver, save_btn)
            time.sleep(3)
            emitter.log('Location saved')
        except:
            pass
        emitter.done('login', 'Signed in to HomeSource')
    except Exception as e:
        errors.fatal(
            step='login',
            code='LOGIN_FAIL',
            message='Could not sign in to HomeSource. Check your username and password.',
            detail=str(e),
        )


# ── Step 1: Batch invoice ─────────────────────────────────────────

def scrape_batch_invoice(driver, wait, delivery_date, emitter, errors):
    date_str = delivery_date.strftime('%B %d, %Y')
    emitter.running('bulk', f'Downloading batch invoice — {date_str}')
    driver.get(f"{BASE_URL}/sales/batch-invoice")
    time.sleep(5)
    try:
        from selenium.webdriver.common.keys import Keys
        date_field = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input.form-control.input[type='text']")
        ))
        driver.execute_script("arguments[0].value = '';", date_field)
        date_field.click()
        time.sleep(0.5)
        date_field.send_keys(Keys.CONTROL + "a")
        date_field.send_keys(Keys.DELETE)
        time.sleep(0.3)
        date_field.send_keys(date_str)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            date_field
        )
        time.sleep(1)
        date_field.send_keys(Keys.RETURN)
        time.sleep(4)
        actual = driver.execute_script("return arguments[0].value;", date_field)
        if date_str.lower() not in (actual or "").lower():
            emitter.log(f'Date retry — field shows: {actual}', 'warn')
            date_field.clear()
            time.sleep(0.3)
            date_field.send_keys(date_str)
            driver.execute_script(
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                date_field
            )
            time.sleep(1)
            date_field.send_keys(Keys.RETURN)
            time.sleep(4)
        else:
            emitter.log(f'Date confirmed: {actual}')
    except Exception as e:
        errors.warn('bulk', 'BULK_DATE_ERROR', f'Could not set date: {e}', str(e))

    try:
        export_btn = driver.find_element(By.XPATH, "//button[@onclick='batchPrintExcel()']")
        js_click(driver, export_btn)
        path = wait_for_download('bulk-invoice', emitter=emitter)
        if not path:
            path = wait_for_download('.xlsx', emitter=emitter)
        if path:
            emitter.done('bulk', 'Batch invoice downloaded')
        else:
            errors.warn('bulk', 'BULK_DOWNLOAD_TIMEOUT',
                        'Batch invoice download timed out — report may be incomplete')
        return path
    except Exception as e:
        errors.warn('bulk', 'BULK_EXPORT_ERROR', f'Batch invoice export failed: {e}', str(e))
        return None




# ── Step 2: Serial inventory ──────────────────────────────────────

def scrape_serial_inventory(driver, wait, emitter, errors):
    emitter.running('serial', 'Downloading serial inventory — full history (open filter off)')
    driver.get(f"{BASE_URL}/inventory/serial")
    time.sleep(4)

    # Open Advanced Filters if not already open
    try:
        adv_btn = driver.find_element(
            By.CSS_SELECTOR, "button[data-target='#serial-filters-collapse']"
        )
        expanded = adv_btn.get_attribute("aria-expanded")
        if expanded != "true":
            js_click(driver, adv_btn)
            wait.until(EC.visibility_of_element_located((By.ID, "serial-filters-collapse")))
            emitter.log('Advanced Filters opened')
        else:
            emitter.log('Advanced Filters already open')
    except Exception as e:
        errors.warn('serial', 'SERIAL_FILTER_ERROR',
                    f'Could not open Advanced Filters: {e}', str(e))

    # Uncheck OpenFilter
    try:
        open_filter = driver.find_element(By.ID, "OpenFilter")
        if open_filter.is_selected():
            js_click(driver, open_filter)
            emitter.log('Open items filter removed — loading full inventory…')
            time.sleep(12)
            emitter.log('Full inventory loaded')
        else:
            emitter.log('Open filter already unchecked')
    except Exception as e:
        errors.warn('serial', 'SERIAL_FILTER_UNCHECK',
                    f'Could not uncheck OpenFilter — export may be incomplete: {e}', str(e))

    # Export
    try:
        export_btn = driver.find_element(
            By.XPATH, "//i[contains(@class,'fa-file-excel-o')]/.."
        )
        js_click(driver, export_btn)
        emitter.log('Serial export triggered — ~5 minutes for full history…')
        path = wait_for_download('serial-number-inventory', timeout=360, emitter=emitter)
        if path:
            emitter.done('serial', 'Serial inventory downloaded')
        else:
            errors.warn('serial', 'SERIAL_DOWNLOAD_TIMEOUT',
                        'Serial inventory download timed out — costs may use CSV fallback')
        return path
    except Exception as e:
        errors.warn('serial', 'SERIAL_EXPORT_ERROR',
                    f'Serial export failed: {e}', str(e))
        return None


# ── Step 3: Orders detail ─────────────────────────────────────────

def scrape_orders_detail(driver, wait, delivery_date, emitter, errors):
    date_str = delivery_date.strftime('%B %d, %Y')
    emitter.running('orders', f'Downloading orders detail — {date_str}')
    driver.get(f"{BASE_URL}/sales/orders")
    time.sleep(4)
    try:
        tab = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//a[@data-toggle='tab' and contains(text(),'Open Orders')]")
        ))
        js_click(driver, tab)
        time.sleep(2)
    except Exception as e:
        errors.warn('orders', 'ORDERS_TAB_ERROR', f'Could not click Open Orders tab: {e}', str(e))
    try:
        Select(driver.find_element(By.NAME, "date-type")).select_by_value("EstimatedDeliveryDate")
        time.sleep(1)
    except Exception as e:
        errors.warn('orders', 'ORDERS_DATETYPE_ERROR', f'Could not set date type: {e}', str(e))
    try:
        d = delivery_date
        js_click(driver, driver.find_element(By.NAME, "dates"))
        time.sleep(2)
        driver.execute_script(f"""
            var el = $('input[name="dates"]');
            if (el.data('daterangepicker')) {{
                var d = new Date({d.year}, {d.month - 1}, {d.day});
                el.data('daterangepicker').setStartDate(d);
                el.data('daterangepicker').setEndDate(d);
                el.data('daterangepicker').updateElement();
            }}
        """)
        time.sleep(1)
        js_click(driver, driver.find_element(By.XPATH, "//button[contains(@class,'applyBtn')]"))
        time.sleep(2)
        emitter.log(f'Date range set: {date_str}')
    except Exception as e:
        errors.warn('orders', 'ORDERS_DATE_ERROR', f'Could not set date range: {e}', str(e))
    try:
        js_click(driver, driver.find_element(By.XPATH, "//span[contains(@class,'fa-download')]/.."))
        time.sleep(1)
        js_click(driver, wait.until(EC.presence_of_element_located((By.ID, "detail-btn"))))
        time.sleep(2)
        js_click(driver, wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[contains(@class,'k-button-solid-primary') and contains(text(),'Export')]")
        )))
        path = wait_for_download('orders-detail', timeout=30, emitter=emitter)
        if path:
            emitter.done('orders', 'Orders detail downloaded')
        else:
            errors.warn('orders', 'ORDERS_DOWNLOAD_TIMEOUT', 'Orders detail download timed out')
        return path
    except Exception as e:
        errors.warn('orders', 'ORDERS_EXPORT_ERROR', f'Orders detail export failed: {e}', str(e))
        return None


# ── Step 4: Route sheet PDF ───────────────────────────────────────

def scrape_route_sheet(driver, delivery_date, emitter, errors):
    date_str = delivery_date.strftime('%m/%d/%Y')
    url = (f"{BASE_URL}/schedule/delivery/routeSheet"
           f"?scheduleDate={date_str}&truckId=all&deliveryType=")
    emitter.running('route', f'Downloading route sheet PDF')
    try:
        session  = get_requests_session(driver)
        response = session.get(url, timeout=30)
        if response.status_code != 200:
            errors.warn('route', 'ROUTE_HTTP_ERROR',
                        f'Route sheet returned HTTP {response.status_code}')
            return None
        if len(response.content) < 1000:
            errors.warn('route', 'ROUTE_EMPTY',
                        'Route sheet response too small — may not be a valid PDF')
            return None
        out_path = os.path.join(INBOX_DIR, f"route_sheet_{delivery_date.strftime('%m%d%Y')}.pdf")
        with open(out_path, 'wb') as f:
            f.write(response.content)
        emitter.done('route', 'Route sheet downloaded')
        return out_path
    except Exception as e:
        errors.warn('route', 'ROUTE_DOWNLOAD_ERROR',
                    f'Route sheet download failed: {e}', str(e))
        return None


# ── Clear inbox ───────────────────────────────────────────────────

def clear_inbox(emitter=None):
    import glob
    cleared = 0
    for f in glob.glob(os.path.join(INBOX_DIR, '*')):
        try:
            os.remove(f)
            cleared += 1
        except:
            pass
    if cleared and emitter:
        emitter.log(f'Inbox cleared: {cleared} old files removed')


# ── Main ──────────────────────────────────────────────────────────

def run(delivery_date, emitter, errors):
    """
    App entry point — called by pipeline.py.

    Args:
        delivery_date: datetime object for the target delivery date
        emitter:       ProgressEmitter from pipeline.py
        errors:        ErrorRegistry from pipeline.py

    Returns:
        dict with keys: bulk_invoice, serial, orders_detail,
                        route_sheet, delivery_date
    """
    from chromedriver_manager import ensure_chromedriver

    results = {
        'delivery_date': delivery_date,
        'bulk_invoice':  None,
        'serial':        None,
        'orders_detail': None,
        'route_sheet':   None,
    }

    # Clear inbox of old files
    clear_inbox(emitter=emitter)

    # Ensure ChromeDriver
    try:
        driver_path = ensure_chromedriver(emit_log=emitter.log)
    except RuntimeError as e:
        errors.fatal('login', str(e).split(':')[0], str(e))

    driver = make_driver(driver_path)
    wait   = WebDriverWait(driver, 40)

    try:
        login(driver, wait, emitter, errors)
        results['bulk_invoice']  = scrape_batch_invoice(driver, wait, delivery_date, emitter, errors)
        results['serial']        = scrape_serial_inventory(driver, wait, emitter, errors)
        results['orders_detail'] = scrape_orders_detail(driver, wait, delivery_date, emitter, errors)
        results['route_sheet']   = scrape_route_sheet(driver, delivery_date, emitter, errors)
    finally:
        driver.quit()

    return results
