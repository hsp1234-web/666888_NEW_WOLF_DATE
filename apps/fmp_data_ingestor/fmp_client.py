from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
import time
from dotenv import load_dotenv
import requests_cache

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configure requests_cache
# Cache API responses to avoid re-fetching data frequently
# Cache will be stored in fmp_cache.sqlite, expire after 1 day
cache_session = requests_cache.CachedSession(
    'fmp_cache', expire_after=timedelta(days=1), backend='sqlite')
# Retry mechanism for requests
# Adapts from https://www.peterbe.com/plog/best-practice-with-retries-with-requests

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    # Will sleep for {backoff factor} * (2 ** ({number of total retries} - 1))
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

# Use cached session for actual API calls
# requests_cache.CachedSession('fmp_cache', expire_after=timedelta(days=1), backend='sqlite', session=http)
cached_http = cache_session
# Re-apply retry strategy to the cached session if not inherited (depends on requests_cache version)
cached_http_adapter = HTTPAdapter(max_retries=retry_strategy)
cached_http.mount("https://", cached_http_adapter)
cached_http.mount("http://", cached_http_adapter)


def get_fmp_api_key():
    """Retrieves the FMP API key from environment variables or Colab Secrets."""
    # Standard environment variable name used by the runner script
    # The runner script will handle populating this from Colab Secret if available.
    api_key = os.getenv("API_KEY_FMP")  # Changed from FMP_API_KEY
    if not api_key:
        logger.error(
            "API_KEY_FMP not found in environment variables. Ensure it's set (e.g., via .env file or Colab Secrets mapped by a runner script).")
        return None
    return api_key


def get_daily_price_data(api_key: str, symbol: str, start_date: str, end_date: str, max_retries=3, backoff_factor=1) -> pd.DataFrame | None:
    """
    Fetches daily historical price data for a given symbol from Financial Modeling Prep.

    Args:
        api_key: Your FMP API key.
        symbol: The stock symbol (e.g., AAPL).
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        max_retries: Maximum number of retries for the API request.
        backoff_factor: Factor to determine the delay between retries.

    Returns:
        A Pandas DataFrame with historical price data (date, open, high, low, close, volume, vwap, change, changePercent),
        or None if an error occurs.
    """
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start_date}&to={end_date}&apikey={api_key}"
    logger.info(
        f"Fetching daily price data for {symbol} from {start_date} to {end_date}")

    current_retry = 0
    while current_retry < max_retries:
        try:
            response = cached_http.get(url, timeout=10)  # Using cached_http
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

            data = response.json()
            if not data or "historical" not in data or not data["historical"]:
                logger.warning(
                    f"No historical data found for {symbol} for the given date range.")
                return None

            df = pd.DataFrame(data["historical"])
            df = df.sort_values(by="date").reset_index(drop=True)
            logger.info(
                f"Successfully fetched {len(df)} rows of daily price data for {symbol}")
            return df

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 401:  # Unauthorized
                logger.error(
                    "API key is invalid or expired. Please check your FMP_API_KEY.")
                return None
            if e.response.status_code == 404:  # Not found
                logger.error(
                    f"Symbol {symbol} not found or no data available for the given range.")
                return None
            if e.response.status_code == 429:  # Too many requests
                logger.warning(
                    f"Rate limit exceeded. Retrying in {backoff_factor * (2 ** current_retry)} seconds...")
            # For other HTTP errors, retry as per strategy
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
        except json.JSONDecodeError:
            logger.error(
                "Failed to decode JSON response. The API might be down or returning malformed data.")
            # No specific retry for JSON decode error unless it's a symptom of a transient network issue handled by Retry

        current_retry += 1
        if current_retry < max_retries:
            # Exponential backoff
            time.sleep(backoff_factor * (2 ** (current_retry - 1)))
        else:
            logger.error(
                f"Max retries reached for {symbol}. Unable to fetch data.")
            return None
    return None


def get_company_profile(api_key: str, symbol: str, cache_days=7) -> dict | None:
    """
    Fetches company profile information for a given symbol from Financial Modeling Prep.

    Args:
        api_key: Your FMP API key.
        symbol: The stock symbol (e.g., AAPL).
        cache_days: Number of days to cache the profile data.

    Returns:
        A dictionary containing company profile information, or None if an error occurs.
    """
    url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={api_key}"
    logger.info(f"Fetching company profile for {symbol}")

    # Update cache expiration for this specific call if different from session default
    # This requires a bit more advanced usage of requests_cache if done per-request with a shared session.
    # For simplicity, we rely on the global cache_session's expire_after or adjust it if needed globally.
    # If fine-grained control per URL is needed, one might use different sessions or context managers.

    try:
        # For profile data, we might want a different cache duration, e.g., 7 days.
        # The global session is set to 1 day. We can override this by creating a specific context.
        with requests_cache.CachedSession(cache_name='fmp_cache', expire_after=timedelta(days=cache_days), backend='sqlite', session=http) as specific_session:
            response = specific_session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if not data:
                logger.warning(f"No profile data found for {symbol}.")
                return None

            # FMP returns a list with one dictionary for profile
            profile_data = data[0] if isinstance(
                data, list) and len(data) > 0 else data
            logger.info(f"Successfully fetched profile data for {symbol}")
            return profile_data

    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error occurred while fetching profile for {symbol}: {e.response.status_code} - {e.response.text}")
        if e.response.status_code == 401:
            logger.error(
                "API key is invalid or expired. Please check your FMP_API_KEY.")
        elif e.response.status_code == 404:
            logger.error(f"Profile for symbol {symbol} not found.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Request failed while fetching profile for {symbol}: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(
            f"Failed to decode JSON response for {symbol} profile. API might be returning malformed data.")
        return None


def get_financial_statements(api_key: str, symbol: str, period="quarter", limit=5, cache_days=7) -> list | None:
    """
    Fetches financial statements (income statements for now) for a given symbol.

    Args:
        api_key: Your FMP API key.
        symbol: The stock symbol.
        period: "quarter" or "annual".
        limit: Number of past periods to fetch.
        cache_days: Number of days to cache the data.

    Returns:
        A list of dictionaries, where each dictionary is a financial statement for a period,
        or None if an error occurs.
    """
    # Note: FMP provides /v3/income-statement, /v3/balance-sheet-statement, /v3/cash-flow-statement
    # This function currently focuses on income statements. Can be expanded.
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period={period}&limit={limit}&apikey={api_key}"
    logger.info(
        f"Fetching {period} income statements for {symbol} (limit {limit})")

    try:
        with requests_cache.CachedSession(cache_name='fmp_cache', expire_after=timedelta(days=cache_days), backend='sqlite', session=http) as specific_session:
            # Longer timeout for potentially larger data
            response = specific_session.get(url, timeout=15)
            response.raise_for_status()

            data = response.json()
            if not data or not isinstance(data, list) or len(data) == 0:
                logger.warning(
                    f"No {period} income statements found for {symbol} with limit {limit}.")
                return None

            logger.info(
                f"Successfully fetched {len(data)} {period} income statements for {symbol}")
            return data  # FMP returns a list of statements

    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error occurred while fetching financials for {symbol}: {e.response.status_code} - {e.response.text}")
        if e.response.status_code == 401:
            logger.error(
                "API key is invalid or expired. Please check your FMP_API_KEY.")
        elif e.response.status_code == 404:
            logger.error(
                f"Financials for symbol {symbol} (period {period}) not found.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Request failed while fetching financials for {symbol}: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(
            f"Failed to decode JSON response for {symbol} financials. API might be returning malformed data.")
        return None
