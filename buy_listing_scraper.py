"""
Buy listing scraper for Property Finder Qatar.
Uses Playwright to open the buy page, sort by newest, paginate, and collect listings
until listed_date (ISO) is older than days_back. Page structure from www.propertyfinder.qa
buy page: __NEXT_DATA__ has props.pageProps.searchResult.listings (each item has "property")
and searchResult.meta.total_count.
"""
import json
import re
import time
from datetime import datetime, timezone  # for listed_date_to_days_ago (ISO listed_date)
from database import BUY_LISTINGS_COLUMNS

# All columns except scrape_run_id (set by app)
LISTING_KEYS = [c for c in BUY_LISTINGS_COLUMNS if c != 'scrape_run_id']


def parse_listed_ago_days(text):
    """
    Parse "Listed X hours ago" / "Listed X days ago" / "Listed more than 6 months ago"
    into days (float). Returns None if unparseable; very large number for "months ago" so we stop.
    """
    if not text or not isinstance(text, str):
        return None
    text = text.strip().lower()
    # "Listed 5 hours ago" -> 5/24
    m = re.search(r'listed\s+(\d+)\s+hour', text)
    if m:
        return int(m.group(1)) / 24.0
    # "Listed 2 days ago"
    m = re.search(r'listed\s+(\d+)\s+day', text)
    if m:
        return float(m.group(1))
    # "Listed more than 6 months ago" or "X months ago"
    m = re.search(r'month|more than', text)
    if m:
        return 999.0
    # "Listed 1 week ago" etc
    m = re.search(r'listed\s+(\d+)\s+week', text)
    if m:
        return float(m.group(1)) * 7
    return None


def listed_date_to_days_ago(iso_date_str):
    """
    Parse ISO listed_date (e.g. '2025-12-23T13:16:56Z') and return days ago (float).
    Returns None if unparseable; 999.0 for future dates so we don't stop.
    """
    if not iso_date_str or not isinstance(iso_date_str, str):
        return None
    try:
        s = iso_date_str.strip().replace('Z', '+00:00')
        if '+' not in s and 'Z' not in iso_date_str:
            s = s + '+00:00' if 'T' in s else s
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - dt
        if delta.total_seconds() < 0:
            return 999.0
        return delta.total_seconds() / 86400.0
    except (ValueError, TypeError):
        return None


def safe_get(obj, *keys, default=None):
    """Navigate nested dict: safe_get(d, 'price', 'value') -> d.get('price', {}).get('value')"""
    for k in keys:
        if obj is None or not isinstance(obj, dict):
            return default
        obj = obj.get(k)
    return obj if obj is not None else default


def listing_to_row(item):
    """Map a single listing object from API/__NEXT_DATA__ to a flat dict with BUY_LISTINGS_COLUMNS keys (no scrape_run_id)."""
    def num(v):
        if v is None: return None
        if isinstance(v, (int, float)): return v
        try: return float(str(v).replace(',', ''))
        except (ValueError, TypeError): return None

    def str_or_none(v):
        if v is None: return None
        s = str(v).strip()
        return s if s else None

    def bool_or_none(v):
        if v is None: return None
        if isinstance(v, bool): return v
        if isinstance(v, str): return v.lower() in ('true', '1', 'yes')
        return bool(v)

    # When item IS the property dict (searchResult.listings[].property), prop = {}; use item for all
    price = item.get('price') or {}
    prop = item.get('property') or {}
    loc = item.get('location') or prop.get('location') or {}
    coords = loc.get('coordinates') or {}
    agent = item.get('agent') or prop.get('agent') or {}
    broker = item.get('broker') or prop.get('broker') or {}
    # Prefer item for property-level fields (item may be the property object)
    p = prop if prop else item

    # contact_options and amenities as JSON strings
    contact_opts = item.get('contact_options') or prop.get('contact_options')
    if contact_opts is not None and not isinstance(contact_opts, str):
        contact_opts = json.dumps(contact_opts) if contact_opts else None
    am = item.get('amenities') or prop.get('amenities')
    if am is not None and not isinstance(am, str):
        am = json.dumps(am) if am else None
    # Property Finder API: images = [{small, medium, large, classification_label}] or [{url, link}]
    imgs = item.get('images') or prop.get('images') or prop.get('image') or []
    if isinstance(imgs, list) and imgs and not isinstance(imgs[0], str):
        urls = []
        for x in imgs:
            if not isinstance(x, dict):
                continue
            url = x.get('medium') or x.get('large') or x.get('small') or x.get('url') or x.get('link')
            if url:
                urls.append(url)
        imgs = urls
    if not isinstance(imgs, str):
        imgs = json.dumps(imgs) if imgs else None

    row = {
        'property_id': str_or_none(prop.get('id') or item.get('id')),
        'reference': str_or_none(item.get('reference') or prop.get('reference')),
        'title': str_or_none(item.get('title') or prop.get('title')),
        'property_type': str_or_none(item.get('property_type') or prop.get('property_type')),
        'offering_type': str_or_none(item.get('offering_type')),
        'description': str_or_none(item.get('description') or prop.get('description')),
        'price_value': num(safe_get(price, 'value') or price.get('value')),
        'price_currency': str_or_none(price.get('currency')),
        'price_is_hidden': bool_or_none(price.get('is_hidden')),
        'price_period': str_or_none(price.get('period')),
        'property_video_url': str_or_none(p.get('video_url')),
        'property_has_view_360': bool_or_none(p.get('has_view_360')),
        'size_value': num((item.get('size') or {}).get('value') or (p.get('size') or {}).get('value')),
        'size_unit': str_or_none((item.get('size') or {}).get('unit') or (p.get('size') or {}).get('unit')),
        'bedrooms': num(item.get('bedrooms') or p.get('bedrooms')),
        'bathrooms': num(item.get('bathrooms') or p.get('bathrooms')),
        'furnished': str_or_none(item.get('furnished') or p.get('furnished')),
        'completion_status': str_or_none(item.get('completion_status') or p.get('completion_status')),
        'location_id': str_or_none(loc.get('id')),
        'location_path': str_or_none(loc.get('path')),
        'location_type': str_or_none(loc.get('type')),
        'location_full_name': str_or_none(loc.get('full_name')),
        'location_name': str_or_none(loc.get('name')),
        'location_lat': num(coords.get('lat')),
        'location_lon': num(coords.get('lon')),
        'amenities': am,
        'is_available': bool_or_none(item.get('is_available') or p.get('is_available')),
        'is_new_insert': bool_or_none(item.get('is_new_insert') or p.get('is_new_insert')),
        'listed_date': str_or_none(item.get('listed_date') or p.get('listed_date') or item.get('time_ago') or p.get('time_ago')),
        'live_viewing': str_or_none(item.get('live_viewing') or p.get('live_viewing')),
        'qs': str_or_none(item.get('qs') or p.get('qs')),
        'rsp': str_or_none(item.get('rsp') or p.get('rsp')),
        'rss': str_or_none(item.get('rss') or p.get('rss')),
        'property_is_available': bool_or_none(p.get('is_available')),
        'property_is_verified': bool_or_none(p.get('is_verified')),
        'property_is_direct_from_developer': bool_or_none(p.get('is_direct_from_developer')),
        'property_is_new_construction': bool_or_none(p.get('is_new_construction')),
        'property_is_featured': bool_or_none(p.get('is_featured')),
        'property_is_premium': bool_or_none(p.get('is_premium')),
        'property_is_exclusive': bool_or_none(p.get('is_exclusive')),
        'property_is_broker_project_property': bool_or_none(p.get('is_broker_project_property')),
        'property_is_smart_ad': bool_or_none(p.get('is_smart_ad')),
        'property_is_spotlight_listing': bool_or_none(p.get('is_spotlight_listing')),
        'property_is_claimed_by_agent': bool_or_none(p.get('is_claimed_by_agent')),
        'property_is_under_offer_by_competitor': bool_or_none(p.get('is_under_offer_by_competitor')),
        'property_is_community_expert': bool_or_none(p.get('is_community_expert')),
        'property_is_cts': bool_or_none(p.get('is_cts')),
        'agent_is_super_agent': bool_or_none(agent.get('is_super_agent')),
        'broker_name': str_or_none(broker.get('name')),
        'listing_type': str_or_none(item.get('listing_type') or p.get('listing_type')),
        'category_id': str_or_none(item.get('category_id') or p.get('category_id')),
        'property_images': imgs,
        'property_type_id': str_or_none(p.get('property_type_id') or item.get('property_type_id')),
        'property_utilities_price_type': str_or_none(p.get('utilities_price_type')),
        'contact_options': contact_opts,
        'agent_id': str_or_none(agent.get('id')),
        'agent_user_id': str_or_none(agent.get('user_id')),
        'agent_name': str_or_none(agent.get('name')),
        'agent_image': str_or_none(agent.get('image')),
        'agent_languages': str_or_none(agent.get('languages')) if isinstance(agent.get('languages'), str) else json.dumps(agent.get('languages')) if agent.get('languages') else None,
        'broker_logo': str_or_none(broker.get('logo')),
        'agent_email': str_or_none(agent.get('email')),
        'broker_id': str_or_none(broker.get('id')),
        'broker_email': str_or_none(broker.get('email')),
        'broker_phone': str_or_none(broker.get('phone')),
        'broker_address': str_or_none(broker.get('address')),
    }
    # Ensure all LISTING_KEYS present
    for k in LISTING_KEYS:
        if k not in row:
            row[k] = None
    return row


def extract_total_and_listings_from_next_data(data):
    """
    Extract total count and listings from __NEXT_DATA__. Property Finder QA structure:
    props.pageProps.searchResult.listings (each item has "property") and searchResult.meta.total_count.
    Returns (total_count, list_of_property_dicts) - each listing is the inner "property" object.
    """
    total = None
    listings = []
    try:
        props = data.get('props', {})
        page_props = props.get('pageProps', {})
        # Property Finder QA: pageProps.searchResult.listings, searchResult.meta.total_count
        sr = page_props.get('searchResult')
        if isinstance(sr, dict):
            meta = sr.get('meta') or {}
            total = meta.get('total_count') or meta.get('total') or meta.get('count')
            raw_list = sr.get('listings') or sr.get('results') or sr.get('data') or []
            # Each raw item has "property" key with the listing data
            listings = [item.get('property') or item for item in raw_list if isinstance(item, dict)]
        if not listings and 'searchResults' in page_props:
            srr = page_props['searchResults']
            total = srr.get('totalCount') or srr.get('total') or srr.get('count')
            listings = srr.get('results') or srr.get('data') or srr.get('listings') or []
        if not listings and 'search' in page_props:
            search = page_props['search']
            total = search.get('totalCount') or search.get('total')
            listings = search.get('results') or search.get('data') or search.get('listings') or []
        if not listings and 'listings' in page_props:
            listings = page_props['listings']
            total = page_props.get('totalCount') or page_props.get('total') or len(listings)
        if total is not None and not isinstance(total, int):
            try:
                total = int(str(total).replace(',', ''))
            except ValueError:
                total = None
    except Exception:
        pass
    return total, listings


def extract_total_from_page_content(html):
    """Extract total from page: span[aria-label='Search results count'] contains '8,957 properties', or metaTitle."""
    # Span with aria-label="Search results count" contains "8,957 properties"
    m = re.search(r'aria-label=["\']Search results count["\'][^>]*>\s*([0-9,]+)\s*propert', html, re.I)
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except ValueError:
            pass
    # "8,957 properties" or "8957 properties" near "Properties for sale in Qatar"
    m = re.search(r'Properties for sale in Qatar[^0-9]*([0-9,]+)\s*propert', html, re.I | re.DOTALL)
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except ValueError:
            pass
    # Meta title: "Properties for sale in Qatar - 8,957 Properties for sale"
    m = re.search(r'([0-9,]+)\s*Propert(?:y|ies) for sale', html, re.I)
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except ValueError:
            pass
    return None


def run_buy_listing_scrape(session_id, days_back, progress_storage):
    """
    Run the buy listing scraper. Updates progress_storage[session_id] during execution.
    Returns (list_of_flat_listing_dicts, total_properties_for_sale).
    """
    progress_data = progress_storage[session_id]
    progress_data['current_action'] = 'Launching browser...'

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError("playwright not installed. Run: pip install playwright && playwright install chromium")

    all_listings = []
    total_properties_for_sale = None
    page_num = 1

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        try:
            url = 'https://www.propertyfinder.qa/en/buy/properties-for-sale.html'
            progress_data['current_action'] = 'Loading buy page...'
            page.goto(url, wait_until='networkidle', timeout=60000)
            time.sleep(1.5)

            html = page.content()
            total_properties_for_sale = extract_total_from_page_content(html)

            # Try __NEXT_DATA__ for total if not from text
            script = page.locator('script#__NEXT_DATA__').first
            if script.count():
                try:
                    nd_text = script.text_content()
                    if nd_text:
                        nd = json.loads(nd_text)
                        t, _ = extract_total_and_listings_from_next_data(nd)
                        if t is not None:
                            total_properties_for_sale = t
                except Exception:
                    pass

            progress_data['total_properties_for_sale'] = total_properties_for_sale
            progress_data['current_action'] = 'Sorting by Newest...'

            # Try URL-based sort first (avoids fragile click/timeout); nd = Newest per Property Finder filterChoices
            sort_url = url + '?sort=nd'
            try:
                page.goto(sort_url, wait_until='networkidle', timeout=60000)
                time.sleep(1.5)
            except Exception:
                # Fallback: try UI click (can timeout on Render if element not actionable)
                try:
                    sort_trigger = page.locator('[data-testid="filters-sort"]').first
                    if sort_trigger.count():
                        sort_trigger.click(timeout=10000)
                        time.sleep(0.7)
                        newest = page.locator('[role="option"]:has-text("Newest"), [data-testid*="sort"]:has-text("Newest"), button:has-text("Newest"), a:has-text("Newest")').first
                        if newest.count():
                            newest.click(timeout=10000)
                        else:
                            page.get_by_text("Newest", exact=False).first.click(timeout=10000)
                        time.sleep(2)
                except Exception:
                    # Re-navigate to base URL and continue without sort
                    page.goto(url, wait_until='networkidle', timeout=60000)
                    time.sleep(1.5)

            should_stop = False
            while not should_stop:
                progress_data['current_page'] = page_num
                progress_data['current_action'] = f'Fetching page {page_num}...'
                progress_data['listings_scraped'] = len(all_listings)

                html = page.content()
                # Parse __NEXT_DATA__
                nd_script = page.locator('script#__NEXT_DATA__').first
                if not nd_script.count():
                    break
                nd_text = nd_script.text_content()
                if not nd_text:
                    break
                try:
                    nd = json.loads(nd_text)
                except json.JSONDecodeError:
                    break
                _, page_listings = extract_total_and_listings_from_next_data(nd)
                if not page_listings:
                    break

                for item in page_listings:
                    # item is the property dict (from searchResult.listings[].property)
                    # Prefer ISO listed_date for accurate days_ago; fallback to "Listed X ago" text
                    listed_date_iso = item.get('listed_date') if isinstance(item, dict) else None
                    listed_days = listed_date_to_days_ago(listed_date_iso)
                    if listed_days is None:
                        listed_ago_text = item.get('time_ago') or (item.get('property') or {}).get('time_ago') or ''
                        if isinstance(listed_ago_text, dict):
                            listed_ago_text = listed_ago_text.get('en') or listed_ago_text.get('text') or ''
                        listed_days = parse_listed_ago_days(str(listed_ago_text))
                    if listed_days is not None and listed_days > days_back:
                        should_stop = True
                        break
                    row = listing_to_row(item)
                    row['listed_date'] = listed_date_iso or row.get('listed_date')
                    all_listings.append(row)

                progress_data['listings_scraped'] = len(all_listings)
                progress_data['current_action'] = f'Page {page_num} - {len(all_listings)} listings so far'

                if should_stop:
                    break

                # Check if next page exists; navigate explicitly with sort preserved (?sort=nd&page=N)
                next_link = page.locator('[data-testid="pagination-page-next-link"]').first
                if not next_link.count():
                    next_link = page.locator('a[href*="page="]:has-text("Next"), [aria-label="Go to next page"]').first
                if not next_link.count():
                    break
                page_num += 1
                next_page_url = url + f'?sort=nd&page={page_num}'
                try:
                    page.goto(next_page_url, wait_until='networkidle', timeout=60000)
                except Exception:
                    break
                time.sleep(2)

        finally:
            browser.close()

    progress_data['listings_scraped'] = len(all_listings)
    progress_data['total_properties_for_sale'] = total_properties_for_sale
    return all_listings, total_properties_for_sale
