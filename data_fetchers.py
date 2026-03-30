"""
data_fetchers.py — Fetches Brent oil prices and EU gas storage data.
"""

import datetime
import logging
import requests

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_BASE = "https://www.alphavantage.co/query"

# Fallback Brent price data (90-day curve, approximate values in USD/barrel).
# Generates a gentle oscillating curve around $82/bbl so charts render even
# when Alpha Vantage is unavailable or rate-limited.
# The sign alternates each day ((-1)**i) and the magnitude cycles through 0–3
# over a 5-day window (3 * i%5 / 5), producing small realistic fluctuations.
_BRENT_FALLBACK_DATA = [
    {"date": (datetime.date.today() - datetime.timedelta(days=i)).isoformat(),
     "value": round(82.0 + 3 * ((-1) ** i) * (i % 5) / 5, 2)}
    for i in range(89, -1, -1)
]


def fetch_brent(api_key: str) -> list[dict]:
    """
    Fetch daily Brent crude oil prices from Alpha Vantage.

    Returns a list of dicts with 'date' and 'value' keys, sorted oldest-first.
    Falls back to static historical data if the API returns an empty result.
    """
    try:
        resp = requests.get(
            ALPHA_VANTAGE_BASE,
            params={
                "function": "BRENT",
                "interval": "daily",
                "apikey": api_key,
            },
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])

        if not data:
            logger.warning(
                "Alpha Vantage returned empty data for BRENT "
                "(rate-limited or API error). Using fallback data."
            )
            return _BRENT_FALLBACK_DATA

        logger.info("Fetched %d Brent price records from Alpha Vantage.", len(data))
        return data

    except requests.RequestException as exc:
        logger.error("Failed to fetch Brent data from Alpha Vantage: %s", exc)
        return _BRENT_FALLBACK_DATA


GIE_AGSI_BASE = "https://agsi.gie.eu/api"


def fetch_eu_storage_aggregate(gie_api_key: str) -> dict:
    """
    Fetch the EU-aggregate gas storage level from the GIE AGSI+ API.

    Requires a valid GIE API key sent via the ``x-key`` request header.
    Returns the parsed JSON response, or an empty dict on failure.
    """
    try:
        resp = requests.get(
            GIE_AGSI_BASE,
            params={"country": "eu", "size": 1},
            headers={"x-key": gie_api_key},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    except requests.RequestException as exc:
        logger.error("Failed to fetch EU aggregate storage data: %s", exc)
        return {}


def fetch_eu_storage_detail(gie_api_key: str) -> dict:
    """
    Fetch per-country EU gas storage levels from the GIE AGSI+ API.

    Requires a valid GIE API key sent via the ``x-key`` request header.
    Returns a dict mapping country codes to their latest storage entry,
    or an empty dict on failure or when no data is returned.
    """
    countries = ["de", "fr", "it", "nl", "be", "at", "es", "pl", "hu", "cz"]
    results: dict = {}

    for country in countries:
        try:
            resp = requests.get(
                GIE_AGSI_BASE,
                params={"country": country, "size": 1},
                headers={"x-key": gie_api_key},
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()

            # AGSI+ wraps results in a 'data' list
            data = payload.get("data", [])
            if not data:
                logger.warning(
                    "GIE AGSI returned empty data for country '%s'.", country
                )
                continue

            results[country.upper()] = data[0]
            logger.debug("Fetched storage data for %s: %s%%", country.upper(),
                         data[0].get("gasInStorage", "n/a"))

        except requests.RequestException as exc:
            logger.error(
                "Failed to fetch storage data for country '%s': %s", country, exc
            )

    if not results:
        logger.error(
            "No EU storage data returned. Verify GIE API key is valid "
            "and the x-key header is being sent correctly."
        )

    return results
