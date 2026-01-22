import requests
from bs4 import BeautifulSoup
import json

HEADERS = {"User-Agent": "Mozilla/5.0"}

def scrape_page(page):
    print("Requesting page:", page)   # 👈 PUT IT HERE

    url = f"https://www.propertyfinder.qa/en/find-broker/search?page={page}"
    resp = requests.get(url, headers=HEADERS, timeout=10)

    soup = BeautifulSoup(resp.text, "html.parser")

    script = soup.find("script", {"id": "__NEXT_DATA__"})
    data = json.loads(script.text)

    brokers = data["props"]["pageProps"]["brokers"]["data"]

    results = []

    for b in brokers:
        results.append({
            "name": b["name"],
            "total_agents": b["totalAgents"],
            "super_agents": b["totalSuperAgents"],
            "for_sale": b["propertiesResidentialForSaleCount"],
            "for_rent": b["propertiesResidentialForRentCount"],
            "logo": b["logo"]["links"]["desktop"]
        })

    return results
