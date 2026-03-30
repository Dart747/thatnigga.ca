"""
app.py — Hormuz Terminal: energy crisis dashboard.

Displays live Brent crude prices, EU gas storage levels, and AIS vessel
positions in a terminal dashboard powered by the Rich library.

Environment variables (see .env.example):
    AIS_API_KEY       — AIS Stream API key
    ALPHA_VANTAGE_KEY — Alpha Vantage API key for Brent price data
    GIE_API_KEY       — GIE AGSI+ API key for EU gas storage data
"""

import logging
import os
import time

from dotenv import load_dotenv
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import data_fetchers
from ais_feed import AISFeed

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------

load_dotenv()

AIS_KEY = os.getenv("AIS_API_KEY", "")
AV_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
GIE_KEY = os.getenv("GIE_API_KEY", "")

_missing = [name for name, val in [
    ("AIS_API_KEY", AIS_KEY),
    ("ALPHA_VANTAGE_KEY", AV_KEY),
    ("GIE_API_KEY", GIE_KEY),
] if not val]

if _missing:
    logger.warning(
        "The following environment variables are not set: %s. "
        "Some data sources may be unavailable.",
        ", ".join(_missing),
    )

# ---------------------------------------------------------------------------
# Rich helpers
# ---------------------------------------------------------------------------

console = Console()


def _brent_panel(brent_data: list[dict]) -> Panel:
    """Render the last 10 Brent price data points as a simple table."""
    table = Table(title="Brent Crude (USD/bbl)", expand=True, show_lines=True)
    table.add_column("Date", style="cyan", no_wrap=True)
    table.add_column("Price", style="green", justify="right")

    recent = brent_data[-10:] if brent_data else []
    for row in reversed(recent):
        table.add_row(str(row.get("date", "—")), str(row.get("value", "—")))

    return Panel(table, title="[bold yellow]🛢  Brent Prices[/bold yellow]")


def _storage_panel(storage_data: dict) -> Panel:
    """Render EU gas storage fill levels by country."""
    table = Table(title="EU Gas Storage (%)", expand=True, show_lines=True)
    table.add_column("Country", style="cyan", no_wrap=True)
    table.add_column("Fill %", style="magenta", justify="right")
    table.add_column("Full (TWh)", justify="right")
    table.add_column("Date", justify="right")

    if not storage_data:
        table.add_row("[red]No data[/red]", "—", "—", "—")
    else:
        for country, entry in sorted(storage_data.items()):
            fill = entry.get("full", entry.get("gasInStorage", "—"))
            capacity = entry.get("workingGasVolume", "—")
            date = entry.get("gasDayStart", entry.get("date", "—"))
            table.add_row(country, str(fill), str(capacity), str(date))

    return Panel(table, title="[bold blue]🏭  EU Gas Storage[/bold blue]")


def _vessels_panel(vessels: dict) -> Panel:
    """Render the list of tracked vessels."""
    table = Table(title="Tracked Vessels", expand=True, show_lines=True)
    table.add_column("MMSI", style="cyan", no_wrap=True)
    table.add_column("Name", style="white")
    table.add_column("Lat", justify="right")
    table.add_column("Lon", justify="right")
    table.add_column("Speed (kn)", justify="right")

    if not vessels:
        table.add_row("[red]Waiting for AIS data…[/red]", "", "", "", "")
    else:
        for vessel in list(vessels.values())[:20]:
            table.add_row(
                str(vessel.get("mmsi", "—")),
                vessel.get("name", "—"),
                f"{vessel.get('lat', '—'):.4f}" if isinstance(vessel.get("lat"), float) else "—",
                f"{vessel.get('lon', '—'):.4f}" if isinstance(vessel.get("lon"), float) else "—",
                str(vessel.get("sog", "—")),
            )

    return Panel(table, title="[bold green]🚢  Strait of Hormuz Vessels[/bold green]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger.info("Starting Hormuz Terminal…")

    # -- Fetch static / semi-static data -----------------------------------
    logger.info("Fetching Brent crude prices…")
    brent_data = data_fetchers.fetch_brent(AV_KEY)

    logger.info("Fetching EU gas storage levels…")
    eu_storage = data_fetchers.fetch_eu_storage_detail(GIE_KEY)

    # -- Start AIS feed ----------------------------------------------------
    feed = AISFeed(api_key=AIS_KEY)
    feed.start()

    # -- Live dashboard ----------------------------------------------------
    layout = Layout()
    layout.split_column(
        Layout(name="top", ratio=2),
        Layout(name="bottom", ratio=3),
    )
    layout["top"].split_row(
        Layout(name="brent"),
        Layout(name="storage"),
    )

    try:
        with Live(layout, console=console, refresh_per_second=1, screen=True):
            while True:
                layout["brent"].update(_brent_panel(brent_data))
                layout["storage"].update(_storage_panel(eu_storage))
                layout["bottom"].update(_vessels_panel(feed.vessels))
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
    finally:
        feed.stop()


if __name__ == "__main__":
    main()
