from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from typing import Dict, Iterable, List, Optional

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "trading-platform-csvs")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_INPUT_FOLDER = "adjusted-eq-data"
S3_OUTPUT_FOLDER = "returns"
S3_ROE_ROCE_FOLDER = "roe-roce"
S3_SECTOR_FOLDER = "sector"

DEFAULT_INPUT_S3_KEY = None
DEFAULT_OUTPUT_S3_KEY = f"{S3_OUTPUT_FOLDER}/returns_with_full_metrics.csv"
DEFAULT_ROE_ROCE_CACHE_S3_KEY = f"{S3_ROE_ROCE_FOLDER}/roe_roce_cache.json"
DEFAULT_SECTOR_CACHE_S3_KEY = f"{S3_SECTOR_FOLDER}/screener_sector_industry_cache.csv"

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


def _get_s3_client():
    """Initialize and return S3 client with optional credentials."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION", AWS_REGION)
    
    client_kwargs = {"service_name": "s3"}
    if region:
        client_kwargs["region_name"] = region
    
    if access_key and secret_key:
        client_kwargs["aws_access_key_id"] = access_key
        client_kwargs["aws_secret_access_key"] = secret_key
        return boto3.client(**client_kwargs)
    
    try:
        return boto3.client(**client_kwargs)
    except Exception as exc:  # noqa: BLE001
        raise


def _read_csv_from_s3(s3_key: str, bucket: str = S3_BUCKET) -> pd.DataFrame:
    """Read CSV file from S3 and return as DataFrame."""
    try:
        s3_client = _get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        return pd.read_csv(BytesIO(response["Body"].read()))
    except NoCredentialsError:
        raise
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "NoSuchKey":
            return pd.DataFrame()
        raise
    except Exception as exc:  # noqa: BLE001
        raise


def _write_csv_to_s3(df: pd.DataFrame, s3_key: str, bucket: str = S3_BUCKET) -> None:
    """Write DataFrame to S3 as CSV."""
    try:
        s3_client = _get_s3_client()
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
    except Exception as exc:  # noqa: BLE001
        raise


def _read_json_from_s3(s3_key: str, bucket: str = S3_BUCKET) -> Dict:
    """Read JSON file from S3 and return as dict."""
    try:
        s3_client = _get_s3_client()
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "NoSuchKey":
            return {}
        raise
    except Exception as exc:  # noqa: BLE001
        return {}


def _write_json_to_s3(data: Dict, s3_key: str, bucket: str = S3_BUCKET) -> None:
    """Write dict to S3 as JSON."""
    try:
        s3_client = _get_s3_client()
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(data, indent=2, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001
        pass


def _s3_key_exists(s3_key: str, bucket: str = S3_BUCKET) -> bool:
    """Check if S3 key exists."""
    try:
        s3_client = _get_s3_client()
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except NoCredentialsError:
        raise
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "404" or error_code == "NoSuchKey":
            return False
        raise


def _extract_date_from_filename(s3_key: str) -> Optional[str]:
    """Extract date string (YYYY-MM-DD) from filename like 'returns-YYYY-MM-DD.csv'."""
    try:
        filename = s3_key.split("/")[-1]
        if "returns-" in filename and filename.endswith(".csv"):
            date_str = filename.replace("returns-", "").replace(".csv", "")
            # Validate date format
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
    except (ValueError, IndexError, AttributeError):
        pass
    return None


def _find_latest_returns_file(bucket: str = S3_BUCKET, prefix: str = S3_INPUT_FOLDER) -> Optional[str]:
    """Find the latest returns file in S3 bucket matching returns-*.csv pattern."""
    try:
        s3_client = _get_s3_client()
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/returns-",
        )
        
        if "Contents" not in response:
            return None
        
        returns_files = []
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith(".csv") and "returns-" in key:
                try:
                    filename = key.split("/")[-1]
                    date_str = filename.replace("returns-", "").replace(".csv", "")
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    returns_files.append((date_obj, key))
                except (ValueError, IndexError):
                    continue
        
        if not returns_files:
            return None
        
        returns_files.sort(key=lambda x: x[0], reverse=True)
        latest_file = returns_files[0][1]
        return latest_file
        
    except NoCredentialsError:
        raise
    except Exception as exc:  # noqa: BLE001
        return None


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
        cache_s3_key: Optional[str] = None,
        cache_ttl_hours: float = DEFAULT_CACHE_TTL_HOURS,
        max_retries: int = 5,
        timeout_seconds: float = 30.0,
        bucket: str = S3_BUCKET,
    ) -> None:
        self.delay_seconds = max(0.0, delay_seconds)
        self.cache_s3_key = cache_s3_key
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self.timeout = timeout_seconds
        self.bucket = bucket
        self.session = self._configure_session(max_retries)
        self.cache: Dict[str, ScreenerFetchResult] = {}
        if cache_s3_key:
            self._load_cache(cache_s3_key)

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

    def _load_cache(self, cache_s3_key: str) -> None:
        if not _s3_key_exists(cache_s3_key, self.bucket):
            return
        try:
            data = _read_json_from_s3(cache_s3_key, self.bucket)
        except Exception as exc:  # noqa: BLE001
            return
        for key, payload in data.items():
            try:
                result = ScreenerFetchResult.from_cache(payload)
            except Exception as exc:  # noqa: BLE001
                continue
            if self._is_cache_fresh(result):
                self.cache[key] = result

    def _persist_cache(self) -> None:
        if not self.cache_s3_key:
            return
        try:
            payload = {key: result.to_cache() for key, result in self.cache.items()}
            _write_json_to_s3(payload, self.cache_s3_key, self.bucket)
        except Exception as exc:  # noqa: BLE001
            pass

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


def _load_sector_cache(cache_s3_key: str, bucket: str = S3_BUCKET) -> Dict[str, Dict[str, object]]:
    if not _s3_key_exists(cache_s3_key, bucket):
        return {}
    try:
        df = _read_csv_from_s3(cache_s3_key, bucket)
        if df.empty:
            return {}
    except Exception as exc:  # noqa: BLE001
        return {}

    normalized_cols = [c.strip().lower() for c in df.columns]
    df.columns = normalized_cols
    required = {"symbol", "macro", "sector", "industry", "basicindustry"}
    if not required.issubset(set(normalized_cols)):
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


def _write_sector_cache(cache_s3_key: str, cache: Dict[str, Dict[str, object]], bucket: str = S3_BUCKET) -> None:
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
    df = pd.DataFrame(records)
    _write_csv_to_s3(df, cache_s3_key, bucket)


def _fetch_nse_metadata(session: Session, symbol: str) -> Optional[Dict[str, object]]:
    params = {"symbol": symbol}
    for attempt in range(1, NSE_MAX_RETRIES + 1):
        try:
            response: Response = session.get(
                NSE_BASE_QUOTE_URL, params=params, timeout=NSE_REQUEST_TIMEOUT
            )
        except RequestException as exc:
            if attempt == NSE_MAX_RETRIES:
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
                return None
            time.sleep(NSE_THROTTLE_SECONDS * attempt)
            continue

        try:
            payload = response.json()
        except ValueError:
            if attempt == NSE_MAX_RETRIES:
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
    sector_cache_s3_key: str,
    nse_throttle_seconds: float = NSE_THROTTLE_SECONDS,
    bucket: str = S3_BUCKET,
) -> pd.DataFrame:
    if "ISIN" not in df.columns or "Symbol" not in df.columns:
        raise SystemExit("Input CSV must contain 'ISIN' and 'Symbol' columns.")

    sector_cache = _load_sector_cache(sector_cache_s3_key, bucket)
    nse_session = _bootstrap_nse_session()

    sector_rows = []

    for _, row in df.iterrows():
        isin = str(row.get("ISIN")) if not pd.isna(row.get("ISIN")) else None
        symbol = str(row.get("Symbol")) if not pd.isna(row.get("Symbol")) else None

        screener_result = screener_client.fetch(isin=isin, symbol=symbol)

        normalized_symbol = (symbol or "").strip().upper()
        info = dict(sector_cache.get(normalized_symbol, {}))
        nse_keys = ("sector", "industry", "marketCapCrore")
        if any(_is_missing(info.get(key)) for key in nse_keys):
            if normalized_symbol:
                nse_info = _fetch_nse_metadata(nse_session, normalized_symbol) or {}
                if nse_info:
                    info.update(nse_info)
                    sector_cache[normalized_symbol] = info
                    _write_sector_cache(sector_cache_s3_key, sector_cache, bucket)
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

    enrichment_df = pd.DataFrame(sector_rows)
    merged = pd.concat([df.reset_index(drop=True), enrichment_df], axis=1)
    screener_client._persist_cache()
    return merged


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich returns CSV with ROE/ROCE plus sector, industry, and market cap."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="S3 key to source returns CSV (e.g., 'adjusted-eq-data/returns-2025-11-17.csv'). "
        "If not specified, will use the latest returns-*.csv file from the input folder.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_S3_KEY,
        help="S3 key for output CSV (e.g., 'returns/returns_with_full_metrics.csv').",
    )
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
        type=str,
        default=DEFAULT_ROE_ROCE_CACHE_S3_KEY,
        help="S3 key to Screener ROE/ROCE cache JSON file (e.g., 'roe-roce/roe_roce_cache.json').",
    )
    parser.add_argument(
        "--sector-cache",
        type=str,
        default=DEFAULT_SECTOR_CACHE_S3_KEY,
        help="S3 key to NSE sector cache CSV file (e.g., 'sector/screener_sector_industry_cache.csv').",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=S3_BUCKET,
        help="S3 bucket name (default: trading-platform-csvs).",
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

    bucket = args.bucket

    input_s3_key = args.input
    if not input_s3_key:
        try:
            input_s3_key = _find_latest_returns_file(bucket, S3_INPUT_FOLDER)
            if not input_s3_key:
                return 1
        except NoCredentialsError:
            return 1

    try:
        if not _s3_key_exists(input_s3_key, bucket):
            return 1
    except NoCredentialsError:
        return 1

    try:
        df = _read_csv_from_s3(input_s3_key, bucket)
        if df.empty:
            return 1
    except NoCredentialsError:
        return 1
    except Exception as exc:  # noqa: BLE001
        return 1

    # Determine output filename: use date from input file if output not explicitly provided
    output_s3_key = args.output
    if output_s3_key == DEFAULT_OUTPUT_S3_KEY:
        # Extract date from input filename
        date_str = _extract_date_from_filename(input_s3_key)
        if date_str:
            output_s3_key = f"{S3_OUTPUT_FOLDER}/returns-{date_str}.csv"
        else:
            # Fallback to today's date if can't extract from filename
            date_str = datetime.now().strftime("%Y-%m-%d")
            output_s3_key = f"{S3_OUTPUT_FOLDER}/returns-{date_str}.csv"

    screener_client = ScreenerClient(
        delay_seconds=args.delay,
        cache_s3_key=args.roe_cache,
        cache_ttl_hours=args.cache_ttl_hours,
        timeout_seconds=args.timeout,
        max_retries=args.max_retries,
        bucket=bucket,
    )

    try:
        enriched = enrich_returns(
            df,
            screener_client=screener_client,
            sector_cache_s3_key=args.sector_cache,
            nse_throttle_seconds=args.nse_throttle,
            bucket=bucket,
        )
    except Exception as exc:  # noqa: BLE001
        return 1

    try:
        _write_csv_to_s3(enriched, output_s3_key, bucket)
    except NoCredentialsError:
        return 1
    except Exception as exc:  # noqa: BLE001
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())


