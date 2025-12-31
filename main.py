import asyncio
from playwright.async_api import async_playwright
import resend
import json
import os
import re
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse

# --- CONFIGURATION ---
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "re_JBZCpg9i_BWK4PPFr7Fx4UYdV8JBkwxdW")
EMAIL_TO = os.getenv("EMAIL_TO", "tiago@controlle.com")
DATABASE_URL = os.getenv("DATABASE_URL")

CITIES_CONFIG = {
    "Criciúma": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Içara": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Forquilhinha": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Cocal do Sul": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Siderópolis": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Nova Veneza": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Morro da Fumaça": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Urussanga": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Balneário Rincão": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Araranguá": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Balneário Arroio do Silva": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Maracajá": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Turvo": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Jacinto Machado": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/criciuma-e-regiao",
    "Tubarão": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "Capivari de Baixo": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "Laguna": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "Pescaria Brava": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "Gravatal": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "Imaruí": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "Imbituba": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/sul-de-santa-catarina/tubarao-e-regiao",
    "São Joaquim": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/serra-catarinense",
    "Bom Jardim da Serra": "https://www.olx.com.br/autos-e-pecas/carros/estado-sc/serra-catarinense"
}

resend.api_key = RESEND_API_KEY

# --- DATABASE ---
def get_db_connection():
    """Get database connection"""
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)

def init_database():
    """Initialize database tables"""
    if not DATABASE_URL:
        print("No DATABASE_URL found, skipping database initialization")
        return
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        # Create table for seen ads
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_ads (
                ad_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create table for accumulated ads
        cur.execute("""
            CREATE TABLE IF NOT EXISTS accumulated_ads (
                id SERIAL PRIMARY KEY,
                ad_id BIGINT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                price TEXT,
                url TEXT NOT NULL,
                location TEXT,
                city TEXT,
                fipe TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cur.close()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {e}")
        conn.rollback()
    finally:
        conn.close()

def is_ad_seen(ad_id):
    """Check if ad has been seen"""
    if not DATABASE_URL:
        return False
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM seen_ads WHERE ad_id = %s", (ad_id,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"Error checking seen ad: {e}")
        if conn:
            conn.close()
        return False

def mark_ad_seen(ad_id):
    """Mark ad as seen"""
    if not DATABASE_URL:
        return
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO seen_ads (ad_id) VALUES (%s) ON CONFLICT (ad_id) DO NOTHING", (ad_id,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error marking ad seen: {e}")
        if conn:
            conn.rollback()
            conn.close()

def save_accumulated_ad(ad):
    """Save ad to accumulated ads"""
    if not DATABASE_URL:
        return
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO accumulated_ads (ad_id, title, price, url, location, city, fipe)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ad_id) DO NOTHING
        """, (ad['id'], ad['title'], ad['price'], ad['url'], ad['location'], ad['city'], ad['fipe']))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error saving accumulated ad: {e}")
        if conn:
            conn.rollback()
            conn.close()

def get_accumulated_ads():
    """Get all accumulated ads"""
    if not DATABASE_URL:
        return []
    
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT ad_id as id, title, price, url, location, city, fipe, created_at
            FROM accumulated_ads
            ORDER BY created_at DESC
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        # Convert to list of dicts and format datetime
        ads = []
        for row in results:
            ad = dict(row)
            if 'created_at' in ad and ad['created_at']:
                ad['created_at'] = ad['created_at'].isoformat()
            ads.append(ad)
        
        return ads
    except Exception as e:
        print(f"Error getting accumulated ads: {e}")
        if conn:
            conn.close()
        return []

# --- UTILS ---
def get_now_br():
    return datetime.now(timezone(timedelta(hours=-3)))

def parse_price(price_str):
    if not price_str: return 0.0
    try:
        clean_str = re.sub(r'[^\d]', '', price_str)
        return float(clean_str)
    except:
        return 0.0

def format_currency(value):
    return f"R$ {value:,.0f}".replace(',', '.')

# --- SCRAPER ---
async def get_ad_details(browser, ad_url):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    page = await context.new_page()
    fipe_price = None
    try:
        await page.goto(ad_url, wait_until="networkidle", timeout=45000)
        await asyncio.sleep(3)
        data_str = await page.evaluate("() => document.getElementById('__NEXT_DATA__')?.textContent")
        if data_str:
            data = json.loads(data_str)
            ad_data = data.get('props', {}).get('pageProps', {}).get('ad', {})
            fipe_val = ad_data.get('priceReference', {}).get('fipePrice')
            if fipe_val and fipe_val > 1000:
                fipe_price = format_currency(fipe_val)
        if not fipe_price:
            fipe_price = await page.evaluate('''() => {
                const allElements = document.querySelectorAll('span, p, div');
                for (let el of allElements) {
                    if (el.innerText && el.innerText.trim() === 'PREÇO FIPE') {
                        let parent = el.parentElement;
                        if (parent) {
                            const priceMatch = parent.innerText.match(/R\$\s*[0-9.]+/);
                            if (priceMatch) return priceMatch[0];
                        }
                    }
                }
                return null;
            }''')
    except Exception as e:
        print(f"Error visiting ad {ad_url}: {e}")
    finally:
        await page.close()
        await context.close()
    return fipe_price

async def scrape_region(browser, url, target_cities):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)
        data_str = await page.evaluate("() => document.getElementById('__NEXT_DATA__')?.textContent")
        if not data_str:
            await page.close()
            await context.close()
            return []
        data = json.loads(data_str)
        ads = data.get('props', {}).get('pageProps', {}).get('ads', [])
        results = []
        for ad in ads:
            ad_url = ad.get('url')
            title = ad.get('title')
            price_str = ad.get('price', 'N/A')
            location = ad.get('location', 'N/A')
            if ad_url and title:
                matched_city = next((city for city in target_cities if city.lower() in location.lower()), None)
                if matched_city:
                    results.append({
                        'id': ad.get('listId'),
                        'title': title,
                        'price': price_str,
                        'url': ad_url,
                        'location': location,
                        'city': matched_city
                    })
        await page.close()
        await context.close()
        return results
    except Exception as e:
        print(f"Error scraping region {url}: {e}")
        await page.close()
        await context.close()
        return []

def send_email(new_ads):
    if not new_ads: return
    now_br = get_now_br()
    html_content = f"<h1>Novos Carros Encontrados - {now_br.strftime('%d/%m/%Y %H:%M')}</h1>"
    html_content += "<table border='1' style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>"
    html_content += "<tr style='background-color: #6e0ad6; color: white;'><th>Cidade</th><th>Título</th><th>Preço OLX</th><th>Preço FIPE</th><th>Diferença</th><th>Link</th></tr>"
    for ad in new_ads:
        diff_val = parse_price(ad['price']) - parse_price(ad['fipe'])
        diff_str = f"R$ {diff_val:,.0f}".replace(',', '.')
        diff_color = "green" if diff_val < 0 else "red"
        if ad['fipe'] == "Não informado": 
            diff_str = "N/A"
            diff_color = "black"
        html_content += f"<tr><td style='padding: 8px;'>{ad['city']}</td><td style='padding: 8px;'>{ad['title']}</td><td style='padding: 8px;'>{ad['price']}</td><td style='padding: 8px;'>{ad['fipe']}</td><td style='padding: 8px; color: {diff_color}; font-weight: bold;'>{diff_str}</td><td style='padding: 8px;'><a href='{ad['url']}'>Ver</a></td></tr>"
    html_content += "</table>"
    try:
        resend.Emails.send({
            "from": "OLX Monitor <onboarding@resend.dev>",
            "to": [EMAIL_TO],
            "subject": f"Monitor OLX: {len(new_ads)} novos carros!",
            "html": html_content,
        })
        print(f"Email sent successfully with {len(new_ads)} ads")
    except Exception as e:
        print(f"Error sending email: {e}")

async def run_monitor():
    print(f"Starting monitor run at {get_now_br()}")
    all_new_ads = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        regions = {}
        for city, url in CITIES_CONFIG.items():
            if url not in regions: regions[url] = []
            regions[url].append(city)
        for url, target_cities in regions.items():
            ads = await scrape_region(browser, url, target_cities)
            for ad in ads:
                if not is_ad_seen(ad['id']):
                    fipe_price_str = await get_ad_details(browser, ad['url'])
                    ad['fipe'] = fipe_price_str or "Não informado"
                    all_new_ads.append(ad)
                    mark_ad_seen(ad['id'])
                    save_accumulated_ad(ad)
        await browser.close()
    if all_new_ads:
        send_email(all_new_ads)
        print(f"Run finished. {len(all_new_ads)} new ads found.")
    else:
        print("Run finished. No new ads found.")

def monitor_loop():
    while True:
        try:
            asyncio.run(run_monitor())
        except Exception as e:
            print(f"Monitor loop error: {e}")
        # Wait for 1 hour
        time.sleep(3600)

# --- API ---
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    print("=" * 50)
    print("Starting OLX Monitor Service")
    print(f"Time: {get_now_br().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    init_database()
    print("Database initialized")
    # Run monitor once on startup
    print("Running initial monitor scan...")
    try:
        await run_monitor()
        print("Initial scan completed successfully")
    except Exception as e:
        print(f"Initial scan error: {e}")

@app.get("/ads")
async def get_ads():
    return get_accumulated_ads()

@app.get("/")
async def root():
    return {"status": "online", "last_run": get_now_br().strftime('%Y-%m-%d %H:%M:%S')}

@app.post("/trigger")
async def trigger_monitor():
    """Manually trigger the monitor to run immediately"""
    try:
        await run_monitor()
        return {"status": "success", "message": "Monitor executed successfully", "timestamp": get_now_br().strftime('%Y-%m-%d %H:%M:%S')}
    except Exception as e:
        return {"status": "error", "message": str(e), "timestamp": get_now_br().strftime('%Y-%m-%d %H:%M:%S')}

@app.post("/reset")
async def reset_database():
    """Reset the seen ads database (for testing)"""
    if not DATABASE_URL:
        return {"status": "error", "message": "No database configured"}
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM seen_ads")
        conn.commit()
        count = cur.rowcount
        cur.close()
        conn.close()
        return {"status": "success", "message": f"Reset complete. Deleted {count} seen ads.", "timestamp": get_now_br().strftime('%Y-%m-%d %H:%M:%S')}
    except Exception as e:
        return {"status": "error", "message": str(e), "timestamp": get_now_br().strftime('%Y-%m-%d %H:%M:%S')}

if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Start monitor in a separate thread
    threading.Thread(target=monitor_loop, daemon=True).start()
    
    # Start API
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
