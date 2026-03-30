"""
ais_feed.py — Connects to the AIS Stream websocket and emits vessel position updates.
"""

import json
import logging
import threading
import time

import websocket

logger = logging.getLogger(__name__)

AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"

# Bounding box covering the Strait of Hormuz and Persian Gulf
HORMUZ_BOUNDING_BOX = [[21.0, 55.0], [27.5, 60.5]]


class AISFeed:
    """
    Maintains a persistent websocket connection to AIS Stream and
    accumulates the most recently seen vessel positions.
    """

    def __init__(self, api_key: str, on_vessel_update=None):
        self.api_key = api_key
        self.on_vessel_update = on_vessel_update
        self.vessels: dict = {}
        self._ws = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._reconnect_delay = 5  # seconds between reconnect attempts

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self):
        """Start the background websocket thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="ais-feed")
        self._thread.start()
        logger.info("AIS feed thread started.")

    def stop(self):
        """Signal the background thread to stop and close the connection."""
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        logger.info("AIS feed stopped.")

    # ------------------------------------------------------------------
    # Internal websocket management
    # ------------------------------------------------------------------

    def _run(self):
        """Reconnect loop – keeps the feed alive if the connection drops."""
        while self._running:
            logger.info(
                "Connecting to AIS Stream at %s (bounding box: %s)...",
                AIS_STREAM_URL,
                HORMUZ_BOUNDING_BOX,
            )
            try:
                self._ws = websocket.WebSocketApp(
                    AIS_STREAM_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                # run_forever blocks until the connection closes
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                logger.error(
                    "Unexpected exception in AIS websocket run loop: %s", exc,
                    exc_info=True,
                )

            if self._running:
                logger.warning(
                    "AIS websocket disconnected. Reconnecting in %d s...",
                    self._reconnect_delay,
                )
                time.sleep(self._reconnect_delay)

    def _on_open(self, ws):
        """Send subscription message once the connection is established."""
        logger.info("AIS websocket connection opened. Sending subscription.")
        sub = {
            "APIKey": self.api_key,
            "BoundingBoxes": [HORMUZ_BOUNDING_BOX],
            "FilterMessageTypes": ["PositionReport"],
        }
        try:
            ws.send(json.dumps(sub))
            logger.debug("Subscription payload sent: %s", sub)
        except Exception as exc:
            logger.error("Failed to send AIS subscription: %s", exc)

    def _on_message(self, ws, raw_message: str):
        """Parse an incoming AIS message and update the vessel dict."""
        try:
            msg = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            logger.warning("Received non-JSON AIS message (%s): %r", exc, raw_message[:200])
            return

        msg_type = msg.get("MessageType", "")
        if msg_type != "PositionReport":
            logger.debug("Skipping AIS message type: %s", msg_type)
            return

        meta = msg.get("MetaData", {})
        mmsi = meta.get("MMSI") or msg.get("Message", {}).get("PositionReport", {}).get("UserID")
        if not mmsi:
            logger.debug("AIS PositionReport missing MMSI; skipping.")
            return

        position_report = msg.get("Message", {}).get("PositionReport", {})
        vessel = {
            "mmsi": mmsi,
            "name": meta.get("ShipName", "UNKNOWN").strip(),
            "lat": meta.get("latitude", position_report.get("Latitude")),
            "lon": meta.get("longitude", position_report.get("Longitude")),
            "sog": position_report.get("Sog"),
            "cog": position_report.get("Cog"),
            "timestamp": meta.get("time_utc"),
        }
        self.vessels[mmsi] = vessel
        logger.debug("Updated vessel %s (%s): lat=%s lon=%s", mmsi, vessel["name"],
                     vessel["lat"], vessel["lon"])

        if self.on_vessel_update:
            try:
                self.on_vessel_update(vessel)
            except Exception as exc:
                logger.error("on_vessel_update callback raised: %s", exc)

    def _on_error(self, ws, error):
        """Log websocket-level errors with full context."""
        logger.error(
            "AIS websocket error (type=%s): %s",
            type(error).__name__,
            error,
            exc_info=isinstance(error, Exception),
        )

    def _on_close(self, ws, close_status_code, close_msg):
        """Log the close reason so failures are diagnosable."""
        logger.warning(
            "AIS websocket closed — status=%s message=%r",
            close_status_code,
            close_msg,
        )
