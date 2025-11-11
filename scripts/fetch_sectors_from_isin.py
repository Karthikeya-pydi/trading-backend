from __future__ import annotations

import argparse
import math
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests
from requests import Response, Session
from requests.exceptions import RequestException


DEFAULT_INPUT = Path("scripts/screener_output/returns-2025-11-10.csv")
DEFAULT_OUTPUT = Path(
    "scripts/screener_output/returns-2025-11-10_with_sectors.csv"
)
CACHE_FILENAME = "screener_sector_industry_cache.csv"

NSE_BASE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_HOME_URL = "https://www.nseindia.com"
REQUEST_TIMEOUT = 10
MAX_RETRIES = 4
THROTTLE_SECONDS = 0.2


def _bootstrap_session() -> Session:
    """Create a requests session that can talk to NSE APIs."""
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

    try:
        session.get(NSE_HOME_URL, timeout=REQUEST_TIMEOUT)
    except RequestException as err:
        raise SystemExit(f"Failed to initialise NSE session: {err}") from err

    return session

def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, str):
        return not value.strip()
    return False


def _fetch_nse_metadata(session: Session, symbol: str) -> Optional[Dict[str, object]]:
    """Fetch industry metadata and market cap for a given NSE symbol."""
    params = {"symbol": symbol}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response: Response = session.get(
                NSE_BASE_QUOTE_URL, params=params, timeout=REQUEST_TIMEOUT
            )
        except RequestException as err:
            if attempt == MAX_RETRIES:
                print(
                    f"[WARN] {symbol}: request failed ({err}); giving up.",
                    file=sys.stderr,
                )
                return None
            time.sleep(THROTTLE_SECONDS * attempt)
            continue

        if response.status_code == 401 or response.status_code == 403:
            # Refresh cookies and retry.
            try:
                session.get(NSE_HOME_URL, timeout=REQUEST_TIMEOUT)
            except RequestException:
                pass
            time.sleep(THROTTLE_SECONDS * attempt)
            continue

        if response.status_code != 200:
            if attempt == MAX_RETRIES:
                print(
                    f"[WARN] {symbol}: unexpected status {response.status_code}; "
                    "giving up.",
                    file=sys.stderr,
                )
                return None
            time.sleep(THROTTLE_SECONDS * attempt)
            continue

        try:
            payload = response.json()
        except ValueError:
            if attempt == MAX_RETRIES:
                print(
                    f"[WARN] {symbol}: invalid JSON payload; giving up.",
                    file=sys.stderr,
                )
                return None
            time.sleep(THROTTLE_SECONDS * attempt)
            continue

        industry_info = payload.get("industryInfo") or {}
        info = payload.get("info") or {}
        price_info = payload.get("priceInfo") or {}
        security_info = payload.get("securityInfo") or {}

        if not industry_info and "industry" in info:
            # Some symbols expose legacy `info['industry']` only.
            industry_info = {
                "industry": info.get("industry"),
                "sector": info.get("sector") or "",
                "basicIndustry": info.get("industry"),
                "macro": "",
            }

        if not industry_info:
            print(
                f"[WARN] {symbol}: NSE response missing industry info.",
                file=sys.stderr,
            )
            industry_info = {}

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

        if market_cap_inr is not None:
            result["marketCapCrore"] = round(market_cap_inr / 1e7, 2)
        else:
            result["marketCapCrore"] = None

        return result

    return None
def _load_cache(cache_path: Path) -> Dict[str, Dict[str, object]]:
    if not cache_path.exists():
        return {}

    try:
        cache_df = pd.read_csv(cache_path)
    except Exception as err:  # noqa: BLE001
        print(
            f"[WARN] Failed to read cache {cache_path}: {err}. "
            "Starting with empty cache.",
            file=sys.stderr,
        )
        return {}

    normalized_cols = [c.strip().lower() for c in cache_df.columns]
    cache_df.columns = normalized_cols
    required_cols = {"symbol", "macro", "sector", "industry", "basicindustry"}
    if not required_cols.issubset(set(normalized_cols)):
        print(
            f"[WARN] Cache file {cache_path} missing expected columns. "
            "Ignoring existing cache.",
            file=sys.stderr,
        )
        return {}
    result: Dict[str, Dict[str, object]] = {}
    for _, row in cache_df.iterrows():
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        entry: Dict[str, object] = {
            "macro": "" if pd.isna(row.get("macro")) else str(row.get("macro")),
            "sector": "" if pd.isna(row.get("sector")) else str(row.get("sector")),
            "industry": "" if pd.isna(row.get("industry")) else str(row.get("industry")),
            "basicIndustry": "" if pd.isna(row.get("basicindustry")) else str(row.get("basicindustry")),
        }

        market_cap_crore_val = row.get("marketcapcrore")
        if market_cap_crore_val is not None and not pd.isna(market_cap_crore_val):
            entry["marketCapCrore"] = float(market_cap_crore_val)

        result[symbol] = entry
    return result


def _write_cache(cache_path: Path, cache: Dict[str, Dict[str, object]]) -> None:
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


def enrich_returns_file(
    input_path: Path,
    output_path: Path,
    throttle_seconds: float = THROTTLE_SECONDS,
    cache_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Main entry point to enrich the returns CSV with sector metadata."""
    df = pd.read_csv(input_path)

    if "ISIN" not in df.columns or "Symbol" not in df.columns:
        raise SystemExit("Input CSV must contain 'ISIN' and 'Symbol' columns.")

    symbol_map = (
        df[["ISIN", "Symbol"]]
        .dropna(subset=["ISIN", "Symbol"])
        .drop_duplicates(subset=["ISIN"])
    )

    cache_path = cache_path or input_path.parent / CACHE_FILENAME
    cache: Dict[str, Dict[str, object]] = _load_cache(cache_path)
    rows = []

    total_symbols = len(symbol_map)
    nse_session = _bootstrap_session()

    for idx, row in symbol_map.iterrows():
        isin = row["ISIN"].strip()
        symbol = row["Symbol"].strip().upper()

        if not isin or not symbol:
            continue

        info = dict(cache.get(symbol, {}))
        cache_updated = False
        nse_keys = ("macro", "sector", "industry", "basicIndustry", "marketCapCrore")
        if any(_is_missing(info.get(key)) for key in nse_keys):
            nse_info = _fetch_nse_metadata(nse_session, symbol) or {}
            if nse_info:
                info.update(nse_info)
                cache_updated = True
            time.sleep(throttle_seconds)

        cache[symbol] = info
        if cache_updated:
            _write_cache(cache_path, cache)

        sector_display = info.get("sector") or "N/A"
        industry_display = info.get("industry") or "N/A"
        market_cap_crore = info.get("marketCapCrore")
        if isinstance(market_cap_crore, float) and math.isnan(market_cap_crore):
            market_cap_crore = None
        market_cap_str = f"{market_cap_crore:,.0f} Cr" if isinstance(market_cap_crore, (int, float)) else "N/A"

        success_flag = (
            "OK"
            if any(not _is_missing(info.get(key)) for key in ("sector", "industry"))
            else "MISS"
        )
        print(
            f"[{len(rows)+1:>4}/{total_symbols}] {symbol:<20} -> "
            f"{sector_display} | {industry_display} | MC {market_cap_str} "
            f"({success_flag})"
        )

        rows.append(
            {
                "ISIN": isin,
                "symbol_for_lookup": symbol,
                "macro": info.get("macro", ""),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "basicIndustry": info.get("basicIndustry", ""),
                "marketCapCrore": info.get("marketCapCrore"),
            }
        )

    enrichment_df = pd.DataFrame(rows)

    merged = df.merge(
        enrichment_df.drop(columns=["symbol_for_lookup"])[["ISIN", "sector", "industry", "marketCapCrore"]],
        on="ISIN",
        how="left",
    )
    merged.to_csv(output_path, index=False)

    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NSE sector/industry data plus market cap for each ISIN."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input CSV path (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=THROTTLE_SECONDS,
        help="Delay in seconds between NSE requests.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merged = enrich_returns_file(
        args.input, args.output, args.throttle, cache_path=args.output.parent / CACHE_FILENAME
    )
    print(
        f"Enriched data written to {args.output} "
        f"({len(merged)} rows, {merged['ISIN'].nunique()} unique ISINs)."
    )


if __name__ == "__main__":
    main()

