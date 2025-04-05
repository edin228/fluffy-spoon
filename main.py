import yfinance as yf
import pandas as pd
import os
import requests
import logging
import time
from datetime import datetime
from requests.exceptions import Timeout, ConnectionError

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Use logging.DEBUG for more detailed output
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Tickers you want to monitor
TICKERS = ["INTC", "AAPL", "AMD", "SPY", "NVDA", "QQQ", "LMT", "TSLA", "PLTR", "MSFT", "GOOGL"]

# Thresholds to consider an OI change "unusual"
PERCENT_CHANGE_THRESHOLD = 100   # 100%
ABS_CHANGE_THRESHOLD = 2000      # 2,000 contracts

# Where we store the prior day's OI data
HISTORICAL_OI_FILE = "oi_baseline.csv"  # Ensure the working directory is writable

# Your Discord Webhook URL
# NOTE: Replace the placeholder with your actual Discord webhook URL or load it from an environment variable.
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1349931297492963419/AfFPe0MJwjPF4f4vVlzHk_FsfGccuwiBzWzJbqNJtIO4vXI1arjG0dbYtqVw1_eER28z")

# Maximum number of retries for fetching option chain data
MAX_FETCH_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# ---------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------

def load_previous_oi(filename):
    """
    Load previously stored OI data from a CSV into a DataFrame.
    Returns an empty DataFrame if file doesn't exist.
    """
    if os.path.exists(filename):
        logging.info(f"Loading previous OI data from {filename}")
        return pd.read_csv(filename)
    else:
        logging.warning(f"No existing file {filename}; starting fresh.")
        return pd.DataFrame(columns=["date", "ticker", "expiration", "strike", "type", "openInterest"])

def save_current_oi(df, filename):
    """
    Save today's OI data as CSV for next time's comparison.
    """
    df.to_csv(filename, index=False)
    logging.info(f"Saved current OI data to {filename}")

def send_discord_alert(webhook_url, message):
    """
    Sends a message to a Discord channel via a webhook.
    Includes basic error handling.
    """
    payload = {
        "content": message
    }
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code not in (200, 204):
            logging.error(f"Failed to send Discord message. Response: {response.text}")
        else:
            logging.info("Discord alert sent successfully.")
    except (Timeout, ConnectionError) as e:
        logging.error(f"Error sending Discord message: {e}")

def fetch_option_chain_with_retry(ticker_obj, expiry, max_retries=3, delay=2):
    """
    Attempts to fetch option chain data with retries.
    Returns the option_chain if successful, or None if all retries fail.
    """
    for attempt in range(1, max_retries + 1):
        try:
            opt_chain = ticker_obj.option_chain(expiry)
            return opt_chain
        except (Timeout, ConnectionError) as e:
            logging.warning(f"Attempt {attempt} - Network error for {ticker_obj.ticker} {expiry}: {e}")
        except Exception as e:
            logging.warning(f"Attempt {attempt} - Error fetching option chain for {ticker_obj.ticker} {expiry}: {e}")
        if attempt < max_retries:
            logging.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    logging.error(f"All retries failed for {ticker_obj.ticker} {expiry}")
    return None

# ---------------------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------------------

def main():
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 1) Load the "yesterday" baseline
    prev_oi_df = load_previous_oi(HISTORICAL_OI_FILE)

    # Will gather records of today's OI; add date tracking
    current_oi_records = []

    # We'll store alert lines for any suspicious OI changes
    alert_lines = []

    for ticker in TICKERS:
        tk = yf.Ticker(ticker)

        # 2) Get the list of available option expirations from yfinance
        try:
            expirations = tk.options
        except Exception as e:
            logging.error(f"Failed to fetch options list for {ticker}: {e}")
            continue

        # 3) For each expiration, fetch the full option chain (with retries)
        for expiry in expirations:
            opt_chain = fetch_option_chain_with_retry(tk, expiry,
                                                      max_retries=MAX_FETCH_RETRIES,
                                                      delay=RETRY_DELAY_SECONDS)
            if not opt_chain:
                continue

            # Combine calls and puts into one DataFrame, keeping track of the type
            calls = opt_chain.calls.copy()
            calls["type"] = "CALL"
            puts = opt_chain.puts.copy()
            puts["type"] = "PUT"
            all_opts = pd.concat([calls, puts], ignore_index=True)
            all_opts["ticker"] = ticker
            all_opts["expiration"] = expiry

            # 4) Iterate over each option row to compare OI
            for idx, row in all_opts.iterrows():
                strike = row.get("strike", 0)
                opt_type = row.get("type", "")
                oi_today = row.get("openInterest", 0)

                # Record today's data (with date)
                current_oi_records.append({
                    "date": current_date,
                    "ticker": ticker,
                    "expiration": expiry,
                    "strike": strike,
                    "type": opt_type,
                    "openInterest": oi_today
                })

                # Find yesterday's OI (if any)
                mask = (
                    (prev_oi_df["ticker"] == ticker) &
                    (prev_oi_df["expiration"] == expiry) &
                    (prev_oi_df["strike"] == strike) &
                    (prev_oi_df["type"] == opt_type)
                )
                matching_rows = prev_oi_df[mask]
                if not matching_rows.empty:
                    oi_yesterday = matching_rows.iloc[0]["openInterest"]
                else:
                    oi_yesterday = 0

                # Calculate absolute and percentage changes
                abs_change = oi_today - oi_yesterday

                if oi_yesterday == 0:
                    # Mark the percentage change as infinite if there is a jump from 0 to a positive value
                    pct_change = float('inf') if oi_today > 0 else 0
                else:
                    pct_change = (abs_change / oi_yesterday) * 100

                # Evaluate whether thresholds are met
                meets_abs_threshold = abs_change >= ABS_CHANGE_THRESHOLD
                meets_pct_threshold = (oi_yesterday != 0) and (pct_change >= PERCENT_CHANGE_THRESHOLD)

                if meets_abs_threshold or meets_pct_threshold:
                    # Build alert line including the date and clear contract type
                    if oi_yesterday == 0 and oi_today > 0:
                        pct_display = "∞"
                    else:
                        pct_display = f"{pct_change:.1f}%"
                    
                    line = (
                        f"**{ticker}** ({current_date}) - {expiry} {opt_type} Option @ Strike {strike}\n"
                        f"OI changed from {oi_yesterday} to {oi_today} "
                        f"(+{abs_change}, +{pct_display})"
                    )
                    alert_lines.append(line)

    # 5) Turn our records into a DataFrame
    current_oi_df = pd.DataFrame(current_oi_records, columns=["date", "ticker", "expiration", "strike", "type", "openInterest"])

    # 6) If anomalies are found, post them to Discord
    if alert_lines:
        alert_message = "UNUSUAL OI DETECTED:\n" + "\n\n".join(alert_lines)
        send_discord_alert(DISCORD_WEBHOOK_URL, alert_message)
        logging.info(f"Alerts generated:\n{alert_message}")
    else:
        logging.info("No unusual OI changes found.")

    # 7) Save today's OI as the baseline for the next run
    save_current_oi(current_oi_df, HISTORICAL_OI_FILE)

if __name__ == "__main__":
    main()