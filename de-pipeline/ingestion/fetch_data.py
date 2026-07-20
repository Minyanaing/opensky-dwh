"""OpenSky Network API access: OAuth2 client-credentials token management and flight fetches."""

import logging
import time
from datetime import datetime, timezone

import requests

import config

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class TokenManager:
    """Fetches and caches an OpenSky OAuth2 client-credentials token, refreshing before it expires."""

    def __init__(self, client_id=None, client_secret=None):
        self._client_id = client_id or config.OPENSKY_CLIENT_ID
        self._client_secret = client_secret or config.OPENSKY_CLIENT_SECRET
        if not self._client_id or not self._client_secret:
            raise RuntimeError(
                "OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET are not set. Register a client at "
                "https://opensky-network.org/my-opensky (API Client) and put them in .env."
            )
        self._access_token = None
        self._expires_at = 0.0

    def get_token(self):
        if self._access_token is None or time.monotonic() >= self._expires_at:
            self._refresh()
        return self._access_token

    def _refresh(self):
        response = requests.post(
            config.OPENSKY_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["access_token"]
        # Tokens are valid for 30 minutes; refresh a little early to avoid edge-of-expiry 401s.
        self._expires_at = time.monotonic() + min(payload.get("expires_in", 1800), 1800) - 60
        logger.debug("Refreshed OpenSky OAuth2 token")


def _start_of_utc_day(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.timestamp()


def chunk_window(begin, end, max_day_partitions=None):
    """Split [begin, end) (unix seconds) into chunks that each touch at most
    `max_day_partitions` UTC calendar days.

    OpenSky partitions /flights/arrival and /flights/departure data by UTC calendar day and
    rejects a query that spills into a 3rd day-partition - this depends on which day the chunk
    *starts* on, not on its raw duration in hours, so chunk boundaries must be day-aligned.
    """
    max_day_partitions = max_day_partitions or config.MAX_DAY_PARTITIONS_PER_CHUNK
    chunks = []
    chunk_start = begin
    while chunk_start < end:
        day_start = _start_of_utc_day(chunk_start)
        chunk_cap = day_start + max_day_partitions * 86400
        chunk_end = min(chunk_cap, end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    return chunks


def _get(token_manager, path, params):
    url = f"{config.OPENSKY_API_BASE_URL}{path}"
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {"Authorization": f"Bearer {token_manager.get_token()}"}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Request error on %s (attempt %s/%s): %s", path, attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_BACKOFF_SECONDS)
            continue

        if response.status_code == 404:
            return []  # no flights in this window - not an error

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
            wait_seconds = int(response.headers.get("Retry-After", RETRY_BACKOFF_SECONDS))
            logger.warning(
                "HTTP %s on %s, retrying in %ss (attempt %s/%s)",
                response.status_code,
                path,
                wait_seconds,
                attempt,
                MAX_RETRIES,
            )
            time.sleep(wait_seconds)
            continue

        if not response.ok:
            # raise_for_status() alone drops the response body, which is where OpenSky puts the
            # actual reason (e.g. which constraint on begin/end/airport was violated).
            raise requests.HTTPError(
                f"{response.status_code} error calling {path} params={params}: {response.text[:500]}",
                response=response,
            )
        return response.json()

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Exhausted retries calling {path} with params={params}")


def fetch_departures(token_manager, icao, begin, end):
    """One page of /flights/departure — flights that departed `icao` in [begin, end)."""
    return _get(token_manager, "/flights/departure", {"airport": icao, "begin": int(begin), "end": int(end)})


def fetch_arrivals(token_manager, icao, begin, end):
    """One page of /flights/arrival — flights that arrived at `icao` in [begin, end)."""
    return _get(token_manager, "/flights/arrival", {"airport": icao, "begin": int(begin), "end": int(end)})


def fetch_aircraft_flights(token_manager, icao24, begin, end):
    """One page of /flights/aircraft — flights flown by aircraft `icao24` in [begin, end)."""
    return _get(token_manager, "/flights/aircraft", {"icao24": icao24, "begin": int(begin), "end": int(end)})
