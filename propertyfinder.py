import requests
from bs4 import BeautifulSoup
import json
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}

def scrape_broker_detail_url(broker_url):
    """Scrape individual broker detail page to get address and phone
    broker_url can be:
    - Full URL: https://www.propertyfinder.qa/en/broker/...
    - Relative path: /en/broker/...
    - Slug with ID: slug-id
    - Just slug: slug
    """
    try:
        # Construct full URL if needed
        if broker_url.startswith("http"):
            url = broker_url
        elif broker_url.startswith("/"):
            url = f"https://www.propertyfinder.qa{broker_url}"
        else:
            url = f"https://www.propertyfinder.qa/en/broker/{broker_url}"
        
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Find the JSON-LD schema script
        schema_script = soup.find("script", {"id": "broker-detail-schema"})
        
        if schema_script:
            schema_data = json.loads(schema_script.text)
            
            # The schema is an array, first element contains the broker organization data
            if isinstance(schema_data, list) and len(schema_data) > 0:
                org_data = schema_data[0]
                address = org_data.get("address", "").strip()
                phone = org_data.get("telephone", "").strip()
                
                return {
                    "address": address if address else None,
                    "phone": phone if phone else None
                }
        
        return {"address": None, "phone": None}
    except Exception as e:
        print(f"Error scraping broker detail for {broker_url}: {e}")
        return {"address": None, "phone": None}


def scrape_page(page):
    print("Requesting page:", page)

    url = f"https://www.propertyfinder.qa/en/find-broker/search?page={page}"
    resp = requests.get(url, headers=HEADERS, timeout=10)

    soup = BeautifulSoup(resp.text, "html.parser")

    script = soup.find("script", {"id": "__NEXT_DATA__"})
    data = json.loads(script.text)

    brokers = data["props"]["pageProps"]["brokers"]["data"]

    results = []

    for b in brokers:
        # Get basic info first
        broker_info = {
            "name": b["name"],
            "total_agents": b["totalAgents"],
            "super_agents": b["totalSuperAgents"],
            "for_sale": b["propertiesResidentialForSaleCount"],
            "for_rent": b["propertiesResidentialForRentCount"],
            "logo": b["logo"]["links"]["desktop"],
            "address": None,
            "phone": None
        }
        
        # Try to get broker URL or construct it from available fields
        broker_url = None
        
        # Check for direct URL field (try various possible field names)
        if b.get("url"):
            broker_url = b["url"]
        elif b.get("link"):
            broker_url = b["link"]
        elif b.get("href"):
            broker_url = b["href"]
        elif b.get("slug") and b.get("id"):
            # Construct URL from slug and ID
            broker_slug = b["slug"]
            broker_id = b["id"]
            broker_url = f"{broker_slug}-{broker_id}"
        elif b.get("slug"):
            broker_url = b["slug"]
        elif b.get("id"):
            # If only ID, try to construct slug from name
            broker_id = b["id"]
            broker_slug = b["name"].lower().replace(" ", "-").replace("&", "and")
            broker_slug = "".join(c for c in broker_slug if c.isalnum() or c == "-")
            broker_url = f"{broker_slug}-{broker_id}"
        
        # Debug: Print available keys if URL not found (only for first broker to avoid spam)
        if not broker_url and len(results) == 0:
            print(f"  Debug - Available broker fields: {list(b.keys())}")
        
        # Scrape detail page for address and phone if URL available
        if broker_url:
            print(f"  Scraping details for: {broker_info['name']}")
            detail_info = scrape_broker_detail_url(broker_url)
            broker_info["address"] = detail_info.get("address")
            broker_info["phone"] = detail_info.get("phone")
            # Small delay to avoid overwhelming the server
            time.sleep(0.5)
        else:
            print(f"  Warning: Could not determine URL for {broker_info['name']} - skipping detail scrape")
        
        results.append(broker_info)

    return results
