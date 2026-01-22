import requests
from bs4 import BeautifulSoup
import json
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}

def scrape_broker_detail_url(broker_url, broker_name=""):
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
        
        print(f"    Attempting URL: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        # Check if request was successful
        if resp.status_code != 200:
            print(f"    ERROR: HTTP {resp.status_code} for {broker_name}")
            return {"address": None, "phone": None, "error": f"HTTP {resp.status_code}"}
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Find the JSON-LD schema script
        schema_script = soup.find("script", {"id": "broker-detail-schema"})
        
        if schema_script:
            try:
                schema_data = json.loads(schema_script.text)
                
                # The schema is an array, first element contains the broker organization data
                if isinstance(schema_data, list) and len(schema_data) > 0:
                    org_data = schema_data[0]
                    address = org_data.get("address", "").strip()
                    phone = org_data.get("telephone", "").strip()
                    
                    result = {
                        "address": address if address else None,
                        "phone": phone if phone else None
                    }
                    
                    if result["address"] or result["phone"]:
                        print(f"    SUCCESS: Found address={bool(result['address'])}, phone={bool(result['phone'])}")
                    else:
                        print(f"    WARNING: Schema found but address and phone are empty")
                    
                    return result
                else:
                    print(f"    WARNING: Schema is not a list or is empty")
            except json.JSONDecodeError as e:
                print(f"    ERROR: Failed to parse JSON schema: {e}")
        else:
            print(f"    WARNING: broker-detail-schema script not found on page")
        
        return {"address": None, "phone": None}
    except requests.exceptions.Timeout:
        print(f"    ERROR: Request timeout for {broker_name}")
        return {"address": None, "phone": None, "error": "timeout"}
    except requests.exceptions.RequestException as e:
        print(f"    ERROR: Request failed for {broker_name}: {e}")
        return {"address": None, "phone": None, "error": str(e)}
    except Exception as e:
        print(f"    ERROR: Unexpected error for {broker_name}: {e}")
        return {"address": None, "phone": None, "error": str(e)}


def scrape_page(page):
    print("Requesting page:", page)

    url = f"https://www.propertyfinder.qa/en/find-broker/search?page={page}"
    resp = requests.get(url, headers=HEADERS, timeout=10)

    soup = BeautifulSoup(resp.text, "html.parser")

    script = soup.find("script", {"id": "__NEXT_DATA__"})
    data = json.loads(script.text)

    brokers = data["props"]["pageProps"]["brokers"]["data"]

    results = []

    for idx, b in enumerate(brokers):
        broker_name = b["name"]
        print(f"\n  Processing broker {idx + 1}/{len(brokers)}: {broker_name}")
        
        # Get basic info first
        broker_info = {
            "name": broker_name,
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
        url_source = None
        
        # Check for direct URL field (try various possible field names)
        if b.get("url"):
            broker_url = b["url"]
            url_source = "url field"
        elif b.get("link"):
            broker_url = b["link"]
            url_source = "link field"
        elif b.get("href"):
            broker_url = b["href"]
            url_source = "href field"
        elif b.get("slug") and b.get("id"):
            # Construct URL from slug and ID
            broker_slug = b["slug"]
            broker_id = b["id"]
            broker_url = f"{broker_slug}-{broker_id}"
            url_source = "slug+id"
        elif b.get("slug"):
            broker_url = b["slug"]
            url_source = "slug only"
        elif b.get("id"):
            # If only ID, try to construct slug from name
            broker_id = b["id"]
            broker_slug = broker_name.lower().replace(" ", "-").replace("&", "and")
            broker_slug = "".join(c for c in broker_slug if c.isalnum() or c == "-")
            broker_url = f"{broker_slug}-{broker_id}"
            url_source = "constructed from id+name"
        
        # Debug: Print available keys if URL not found
        if not broker_url:
            print(f"    WARNING: Could not determine URL")
            print(f"    Available fields: {list(b.keys())}")
            # Show what we tried
            print(f"    Checked: url={b.get('url')}, link={b.get('link')}, href={b.get('href')}, slug={b.get('slug')}, id={b.get('id')}")
        else:
            print(f"    URL source: {url_source}, URL: {broker_url}")
        
        # Scrape detail page for address and phone if URL available
        if broker_url:
            detail_info = scrape_broker_detail_url(broker_url, broker_name)
            broker_info["address"] = detail_info.get("address")
            broker_info["phone"] = detail_info.get("phone")
            # Small delay to avoid overwhelming the server
            time.sleep(0.5)
        else:
            print(f"    SKIPPED: No URL available for detail scraping")
        
        results.append(broker_info)

    return results
