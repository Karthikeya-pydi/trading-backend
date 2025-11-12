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
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("enrich_returns_full")

DEFAULT_INPUT = Path("scripts") / "screener_output" / "returns-2025-11-12.csv"
DEFAULT_OUTPUT = Path("scripts") / "screener_output" / "returns_with_full_metrics.csv"

ROE_ROCE_CACHE = Path("scripts") / "screener_output" / "roe_roce_cache.json"
SECTOR_CACHE = Path("scripts") / "screener_output" / "screener_sector_industry_cache.csv"

DEFAULT_DELAY_SECONDS = 4.0
DEFAULT_CACHE_TTL_HOURS = 48

NSE_BASE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_HOME_URL = "https://www.nseindia.com"
NSE_REQUEST_TIMEOUT = 10
NSE_MAX_RETRIES = 4
NSE_THROTTLE_SECONDS = 0.2

SCREENER_HEADERS = {
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
    """Fetches ROE/ROCE from Screener with retry and caching."""

    def __init__(
        self,
        *,
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
        session.headers.update(SCREENER_HEADERS)
        return session

    def _load_cache(self, cache_path: Path) -> None:
        if not cache_path.exists():
            return
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to read Screener cache %s: %s", cache_path, exc)
            return
        for key, payload in data.items():
            try:
                result = ScreenerFetchResult.from_cache(payload)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Skipping corrupt Screener cache entry for %s: %s", key, exc)
                continue
            if self._is_cache_fresh(result):
                self.cache[key] = result

    def _persist_cache(self) -> None:
        if not self.cache_path:
            return
        try:
            payload = {key: result.to_cache() for key, result in self.cache.items()}
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to persist Screener cache to %s: %s", self.cache_path, exc)

    def _is_cache_fresh(self, result: ScreenerFetchResult) -> bool:
        return datetime.now(timezone.utc) - result.fetched_at <= self.cache_ttl

    @staticmethod
    def _normalise_symbol(value: Optional[str]) -> Optional[str]:
        if not value or not isinstance(value, str):
            return None
        stripped = value.strip()
        return stripped or None

    def fetch(self, *, isin: Optional[str], symbol: Optional[str]) -> ScreenerFetchResult:
        cache_key = self._normalise_symbol(symbol) or self._normalise_symbol(isin)
        if cache_key and cache_key in self.cache:
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
            url = f"https://www.screener.in/company/{identifier}/"
            try:
                html = self._request(url)
            except Exception as exc:  # noqa: BLE001
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
            LOGGER.warning("Screener returned 404 for %s", url)
            return None
        response.raise_for_status()
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        return response.text

    @staticmethod
    def _parse_ratios(*, html: str, symbol: str, used_identifier: str, url: str) -> ScreenerFetchResult:
        soup = BeautifulSoup(html, "html.parser")
        ratios: Dict[str, str] = {}

        for li in soup.select("#top-ratios li, .top-ratios li"):
            name, value = ScreenerClient._extract_ratio_pair(li)
            if name:
                ratios[name.lower()] = value

        if not any(key in ratios for key in ("roe", "return on equity", "roce", "return on capital employed")):
            ratios_section = soup.find("section", {"id": "ratios"}) or soup.find("div", {"id": "ratios"})
            if ratios_section:
                for row in ratios_section.select("table tr"):
                    name, value = ScreenerClient._extract_ratio_pair(row)
                    if name and name.lower() not in ratios:
                        ratios[name.lower()] = value

        roe = ScreenerClient._parse_percentage(
            ratios.get("roe") or ratios.get("return on equity")
        )
        roce = ScreenerClient._parse_percentage(
            ratios.get("roce") or ratios.get("return on capital employed")
        )

        return ScreenerFetchResult(
            symbol=symbol,
            used_identifier=used_identifier,
            url=url,
            roe_percent=roe,
            roce_percent=roce,
        )

    @staticmethod
    def _extract_ratio_pair(element):
        if element is None:
            return None, None
        spans = element.find_all("span")
        if len(spans) >= 2:
            return spans[0].get_text(strip=True), spans[1].get_text(strip=True)
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
        cleaned = value.replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
        if not match:
            return None
        try:
            return float(match.group())
        except ValueError:
            return None


def _bootstrap_nse_session() -> Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "Connection": "keep-alive",
        }
    )
    session.get(NSE_HOME_URL, timeout=NSE_REQUEST_TIMEOUT)
    return session


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, str):
        return not value.strip()
    return False


def _load_sector_cache(cache_path: Path) -> Dict[str, Dict[str, object]]:
    if not cache_path.exists():
        return {}
    try:
        df = pd.read_csv(cache_path)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to read sector cache %s: %s", cache_path, exc)
        return {}

    normalized_cols = [c.strip().lower() for c in df.columns]
    df.columns = normalized_cols
    required = {"symbol", "macro", "sector", "industry", "basicindustry"}
    if not required.issubset(set(normalized_cols)):
        LOGGER.warning("Sector cache %s missing expected columns. Ignoring.", cache_path)
        return {}

    cache: Dict[str, Dict[str, object]] = {}
    for _, row in df.iterrows():
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        entry: Dict[str, object] = {
            "macro": "" if pd.isna(row.get("macro")) else str(row.get("macro")),
            "sector": "" if pd.isna(row.get("sector")) else str(row.get("sector")),
            "industry": "" if pd.isna(row.get("industry")) else str(row.get("industry")),
            "basicIndustry": "" if pd.isna(row.get("basicindustry")) else str(row.get("basicindustry")),
        }
        market_cap_val = row.get("marketcapcrore")
        if market_cap_val is not None and not pd.isna(market_cap_val):
            entry["marketCapCrore"] = float(market_cap_val)
        cache[symbol] = entry
    return cache


def _write_sector_cache(cache_path: Path, cache: Dict[str, Dict[str, object]]) -> None:
    records = []
    for symbol, info in sorted(cache.items()):
        records.append(
            {
                "Symbol": symbol,
                "Macro": info.get("macro", ""),
                "Sector": info.get("sector", ""),
                "Industry": info.get("industry", ""),
                "BasicIndustry": info.get("basicIndustry", ""),
                "MarketCapCrore": info.get("marketCapCrore"),
            }
        )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(cache_path, index=False)


def _fetch_nse_metadata(session: Session, symbol: str) -> Optional[Dict[str, object]]:
    params = {"symbol": symbol}
    for attempt in range(1, NSE_MAX_RETRIES + 1):
        try:
            response: Response = session.get(
                NSE_BASE_QUOTE_URL, params=params, timeout=NSE_REQUEST_TIMEOUT
            )
        except RequestException as exc:
            if attempt == NSE_MAX_RETRIES:
                LOGGER.warning("%s: NSE request failed (%s); giving up.", symbol, exc)
                return None
            time.sleep(NSE_THROTTLE_SECONDS * attempt)
            continue

        if response.status_code in (401, 403):
            try:
                session.get(NSE_HOME_URL, timeout=NSE_REQUEST_TIMEOUT)
            except RequestException:
                pass
            time.sleep(NSE_THROTTLE_SECONDS * attempt)
            continue

        if response.status_code != 200:
            if attempt == NSE_MAX_RETRIES:
                LOGGER.warning("%s: unexpected NSE status %s", symbol, response.status_code)
                return None
            time.sleep(NSE_THROTTLE_SECONDS * attempt)
            continue

        try:
            payload = response.json()
        except ValueError:
            if attempt == NSE_MAX_RETRIES:
                LOGGER.warning("%s: invalid NSE JSON payload", symbol)
                return None
            time.sleep(NSE_THROTTLE_SECONDS * attempt)
            continue

        industry_info = payload.get("industryInfo") or {}
        info = payload.get("info") or {}
        price_info = payload.get("priceInfo") or {}
        security_info = payload.get("securityInfo") or {}

        if not industry_info and "industry" in info:
            industry_info = {
                "industry": info.get("industry"),
                "sector": info.get("sector") or "",
                "basicIndustry": info.get("industry"),
                "macro": "",
            }

        market_cap_inr = None
        try:
            last_price = price_info.get("lastPrice")
            if isinstance(last_price, str):
                last_price = float(last_price.replace(",", "").strip())
            shares = security_info.get("issuedSize")
            if last_price not in (None, "") and shares not in (None, ""):
                market_cap_inr = float(last_price) * float(shares)
        except (TypeError, ValueError):
            market_cap_inr = None

        result: Dict[str, object] = {
            "macro": industry_info.get("macro") or "",
            "sector": industry_info.get("sector") or "",
            "industry": industry_info.get("industry") or "",
            "basicIndustry": industry_info.get("basicIndustry") or "",
        }
        result["marketCapCrore"] = round(market_cap_inr / 1e7, 2) if market_cap_inr is not None else None
        return result

    return None


def enrich_returns(
    df: pd.DataFrame,
    *,
    screener_client: ScreenerClient,
    sector_cache_path: Path,
    nse_throttle_seconds: float = NSE_THROTTLE_SECONDS,
) -> pd.DataFrame:
    if "ISIN" not in df.columns or "Symbol" not in df.columns:
        raise SystemExit("Input CSV must contain 'ISIN' and 'Symbol' columns.")

    sector_cache = _load_sector_cache(sector_cache_path)
    nse_session = _bootstrap_nse_session()

    sector_rows = []
    total = len(df)

    LOGGER.info("Processing %d rows", total)

    for idx, row in df.iterrows():
        isin = str(row.get("ISIN")) if not pd.isna(row.get("ISIN")) else None
        symbol = str(row.get("Symbol")) if not pd.isna(row.get("Symbol")) else None

        # Screener data
        screener_result = screener_client.fetch(isin=isin, symbol=symbol)

        # NSE metadata
        normalized_symbol = (symbol or "").strip().upper()
        info = dict(sector_cache.get(normalized_symbol, {}))
        nse_keys = ("sector", "industry", "marketCapCrore")
        if any(_is_missing(info.get(key)) for key in nse_keys):
            if normalized_symbol:
                nse_info = _fetch_nse_metadata(nse_session, normalized_symbol) or {}
                if nse_info:
                    info.update(nse_info)
                    sector_cache[normalized_symbol] = info
                    _write_sector_cache(sector_cache_path, sector_cache)
                time.sleep(nse_throttle_seconds)

        sector_rows.append(
            {
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "marketCapCrore": info.get("marketCapCrore"),
                "ROE_percent": screener_result.roe_percent,
                "ROCE_percent": screener_result.roce_percent,
            }
        )

        status_parts = []
        if screener_result.roe_percent is not None:
            status_parts.append(f"ROE={screener_result.roe_percent:.2f}%")
        if screener_result.roce_percent is not None:
            status_parts.append(f"ROCE={screener_result.roce_percent:.2f}%")
        if info.get("sector"):
            status_parts.append(f"Sector={info.get('sector')}")
        if info.get("industry"):
            status_parts.append(f"Industry={info.get('industry')}")
        if info.get("marketCapCrore") is not None:
            status_parts.append(f"MC={info.get('marketCapCrore'):.1f}Cr")
        LOGGER.info("[%d/%d] %s -> %s", idx + 1, total, symbol or isin or "?", ", ".join(status_parts) or "missing")

    enrichment_df = pd.DataFrame(sector_rows)
    merged = pd.concat([df.reset_index(drop=True), enrichment_df], axis=1)
    screener_client._persist_cache()
    return merged


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich returns CSV with ROE/ROCE plus sector, industry, and market cap."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to source returns CSV.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path.")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Delay between Screener requests (seconds).",
    )
    parser.add_argument(
        "--cache-ttl-hours",
        type=float,
        default=DEFAULT_CACHE_TTL_HOURS,
        help="ROE/ROCE cache freshness window in hours.",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="Screener request timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=5, help="Max Screener retries for transient failures.")
    parser.add_argument(
        "--nse-throttle",
        type=float,
        default=NSE_THROTTLE_SECONDS,
        help="Delay between NSE metadata requests.",
    )
    parser.add_argument(
        "--roe-cache",
        type=Path,
        default=ROE_ROCE_CACHE,
        help="Path to Screener ROE/ROCE cache JSON file.",
    )
    parser.add_argument(
        "--sector-cache",
        type=Path,
        default=SECTOR_CACHE,
        help="Path to NSE sector cache CSV file.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (-v for INFO, -vv for DEBUG).",
    )
    return parser.parse_args(argv)


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S")


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    _configure_logging(args.verbose)

    if not args.input.exists():
        LOGGER.error("Input file %s does not exist.", args.input)
        return 1

    try:
        df = pd.read_csv(args.input)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to read %s: %s", args.input, exc)
        return 1

    screener_client = ScreenerClient(
        delay_seconds=args.delay,
        cache_path=args.roe_cache,
        cache_ttl_hours=args.cache_ttl_hours,
        timeout_seconds=args.timeout,
        max_retries=args.max_retries,
    )

    try:
        enriched = enrich_returns(
            df,
            screener_client=screener_client,
            sector_cache_path=args.sector_cache,
            nse_throttle_seconds=args.nse_throttle,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed enrichment: %s", exc)
        return 1

    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        enriched.to_csv(args.output, index=False)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to write %s: %s", args.output, exc)
        return 1

    LOGGER.info(
        "Enriched data written to %s (%d rows, %d unique ISINs).",
        args.output,
        len(enriched),
        enriched["ISIN"].nunique() if "ISIN" in enriched.columns else len(enriched),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())


