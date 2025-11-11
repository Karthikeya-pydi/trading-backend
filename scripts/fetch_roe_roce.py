from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("fetch_roe_roce")

DEFAULT_INPUT = Path("scripts") / "screener_output" / "returns_with_company_names.csv"
DEFAULT_OUTPUT = Path("scripts") / "screener_output" / "returns_with_roe_roce.csv"
DEFAULT_CACHE = Path("scripts") / "screener_output" / "roe_roce_cache.json"

DEFAULT_DELAY_SECONDS = 4.0
DEFAULT_CACHE_TTL_HOURS = 48

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


@dataclass
class ScreenerFetchResult:
    symbol: str
    used_identifier: str
    url: str
    roe_percent: Optional[float] = None
    roce_percent: Optional[float] = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error: Optional[str] = None

    def to_cache(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "used_identifier": self.used_identifier,
            "url": self.url,
            "roe_percent": self.roe_percent,
            "roce_percent": self.roce_percent,
            "fetched_at": self.fetched_at.isoformat(),
            "error": self.error,
        }

    @classmethod
    def from_cache(cls, payload: Dict[str, object]) -> "ScreenerFetchResult":
        fetched_at = datetime.fromisoformat(str(payload["fetched_at"]))
        return cls(
            symbol=str(payload.get("symbol", "")),
            used_identifier=str(payload.get("used_identifier") or ""),
            url=str(payload.get("url") or ""),
            roe_percent=cls._optional_float(payload.get("roe_percent")),
            roce_percent=cls._optional_float(payload.get("roce_percent")),
            fetched_at=fetched_at,
            error=payload.get("error"),
        )

    @staticmethod
    def _optional_float(value: object) -> Optional[float]:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


class ScreenerClient:
    """Fetches and parses Screener pages while handling rate limits and retries."""

    def __init__(
        self,
        delay_seconds: float = DEFAULT_DELAY_SECONDS,
        cache_path: Optional[Path] = None,
        cache_ttl_hours: float = DEFAULT_CACHE_TTL_HOURS,
        max_retries: int = 5,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.delay_seconds = max(0.0, delay_seconds)
        self.cache_path = cache_path
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self.timeout = timeout_seconds
        self.session = self._configure_session(max_retries)
        self.cache: Dict[str, ScreenerFetchResult] = {}
        if cache_path:
            self._load_cache(cache_path)

    @staticmethod
    def _configure_session(max_retries: int) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=2.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(HEADERS)
        return session

    def _load_cache(self, cache_path: Path) -> None:
        if not cache_path.exists():
            return
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            for key, payload in data.items():
                try:
                    result = ScreenerFetchResult.from_cache(payload)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Skipping corrupt cache entry for %s: %s", key, exc)
                    continue
                if self._is_cache_fresh(result):
                    self.cache[key] = result
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to load cache from %s: %s", cache_path, exc)

    def _persist_cache(self) -> None:
        if not self.cache_path:
            return
        try:
            payload = {key: result.to_cache() for key, result in self.cache.items()}
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to write cache to %s: %s", self.cache_path, exc)

    def _is_cache_fresh(self, result: ScreenerFetchResult) -> bool:
        return datetime.now(timezone.utc) - result.fetched_at <= self.cache_ttl

    @staticmethod
    def _normalise_symbol(value: Optional[str]) -> Optional[str]:
        if not value or not isinstance(value, str):
            return None
        value = value.strip()
        return value or None

    def fetch(self, *, isin: Optional[str], symbol: Optional[str]) -> ScreenerFetchResult:
        cache_key = self._normalise_symbol(symbol) or self._normalise_symbol(isin)
        if cache_key and cache_key in self.cache:
            LOGGER.debug("Cache hit for %s", cache_key)
            return self.cache[cache_key]

        identifiers: List[str] = []
        if symbol and symbol.strip():
            identifiers.append(symbol.strip())
        if isin and isin.strip():
            identifier = isin.strip()
            if identifier not in identifiers:
                identifiers.append(identifier)

        last_error = None
        for identifier in identifiers:
            if not identifier:
                continue
            url = f"https://www.screener.in/company/{identifier}/"
            LOGGER.info("Fetching %s", url)
            try:
                html = self._request(url)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to fetch %s: %s", url, exc)
                last_error = str(exc)
                continue
            if not html:
                last_error = "empty response"
                continue

            result = self._parse_ratios(
                html=html,
                symbol=self._normalise_symbol(symbol) or identifier,
                used_identifier=identifier,
                url=url,
            )
            if cache_key:
                self.cache[cache_key] = result
                self._persist_cache()
            return result

        result = ScreenerFetchResult(
            symbol=self._normalise_symbol(symbol) or (identifiers[0] if identifiers else ""),
            used_identifier=identifiers[0] if identifiers else "",
            url=f"https://www.screener.in/company/{identifiers[0]}/" if identifiers else "",
            roe_percent=None,
            roce_percent=None,
            error=last_error or "No identifiers supplied",
        )
        if cache_key:
            self.cache[cache_key] = result
            self._persist_cache()
        return result

    def _request(self, url: str) -> Optional[str]:
        response = self.session.get(url, timeout=self.timeout)
        if response.status_code == 404:
            LOGGER.warning("Received 404 for %s", url)
            return None
        response.raise_for_status()
        time.sleep(self.delay_seconds)
        return response.text

    @staticmethod
    def _parse_ratios(html: str, *, symbol: str, used_identifier: str, url: str) -> ScreenerFetchResult:
        soup = BeautifulSoup(html, "html.parser")

        ratios: Dict[str, str] = {}

        for li in soup.select("#top-ratios li, .top-ratios li"):
            name, value = ScreenerClient._extract_ratio_pair(li)
            if name:
                ratios[name.lower()] = value

        # In some cases ROE/ROCE may only appear inside the table in the ratios section.
        if not any(key in ratios for key in ("roe", "roce")):
            ratios_section = soup.find("section", {"id": "ratios"}) or soup.find("div", {"id": "ratios"})
            if ratios_section:
                for row in ratios_section.select("table tr"):
                    name, value = ScreenerClient._extract_ratio_pair(row)
                    if name and name.lower() not in ratios:
                        ratios[name.lower()] = value

        roe = ScreenerClient._parse_percentage(ratios.get("roe") or ratios.get("return on equity"))
        roce = ScreenerClient._parse_percentage(ratios.get("roce") or ratios.get("return on capital employed"))

        return ScreenerFetchResult(
            symbol=symbol,
            used_identifier=used_identifier,
            url=url,
            roe_percent=roe,
            roce_percent=roce,
        )

    @staticmethod
    def _extract_ratio_pair(element) -> Tuple[Optional[str], Optional[str]]:
        if element is None:
            return None, None
        spans = element.find_all("span")
        if len(spans) >= 2:
            name = spans[0].get_text(strip=True)
            value = spans[1].get_text(strip=True)
            return name, value
        text = element.get_text(" ", strip=True)
        if ":" in text:
            name, value = text.split(":", 1)
            return name.strip(), value.strip()
        cells = element.find_all("td")
        if len(cells) >= 2:
            return cells[0].get_text(strip=True), cells[1].get_text(strip=True)
        return None, None

    @staticmethod
    def _parse_percentage(value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
        if not match:
            return None
        try:
            return float(match.group())
        except ValueError:
            return None


def _configure_logging(verbosity: int) -> None:
    level = logging.INFO if verbosity == 0 else logging.DEBUG if verbosity > 1 else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch ROE/ROCE values from Screener.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to returns CSV.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path.")
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE, help="Cache file path.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay between requests (seconds).")
    parser.add_argument(
        "--cache-ttl-hours",
        type=float,
        default=DEFAULT_CACHE_TTL_HOURS,
        help="Cache freshness window in hours.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum automatic retries for transient failures.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (use twice for DEBUG).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    _configure_logging(args.verbose)

    input_path: Path = args.input
    output_path: Path = args.output
    cache_path: Path = args.cache

    if not input_path.exists():
        LOGGER.error("Input file %s does not exist.", input_path)
        return 1

    try:
        df = pd.read_csv(input_path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to read %s: %s", input_path, exc)
        return 1

    if "ISIN" not in df.columns or "Symbol" not in df.columns:
        LOGGER.error("Input file must contain 'ISIN' and 'Symbol' columns.")
        return 1

    client = ScreenerClient(
        delay_seconds=args.delay,
        cache_path=cache_path,
        cache_ttl_hours=args.cache_ttl_hours,
        timeout_seconds=args.timeout,
        max_retries=args.max_retries,
    )

    roe_values: List[Optional[float]] = []
    roce_values: List[Optional[float]] = []

    total = len(df)
    LOGGER.info("Processing %d rows from %s", total, input_path)

    for idx, row in df.iterrows():
        isin = str(row.get("ISIN")) if not pd.isna(row.get("ISIN")) else None
        symbol = str(row.get("Symbol")) if not pd.isna(row.get("Symbol")) else None
        result = client.fetch(isin=isin, symbol=symbol)
        roe_values.append(result.roe_percent)
        roce_values.append(result.roce_percent)

        status_bits = []
        if result.roe_percent is not None:
            status_bits.append(f"ROE={result.roe_percent:.2f}%")
        if result.roce_percent is not None:
            status_bits.append(f"ROCE={result.roce_percent:.2f}%")
        status = ", ".join(status_bits) if status_bits else f"missing ({result.error})"
        LOGGER.info("[%d/%d] %s -> %s", idx + 1, total, symbol or isin or "?", status)

    df["ROE_percent"] = roe_values
    df["ROCE_percent"] = roce_values

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to write %s: %s", output_path, exc)
        return 1

    LOGGER.info("Saved %s with %d rows", output_path, total)
    if client.cache_path:
        client._persist_cache()
    return 0


if __name__ == "__main__":
    sys.exit(main())

