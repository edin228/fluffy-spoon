import yfinance as yf
import pandas as pd
import os
import requests

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------

# Tickers you want to monitor
TICKERS = ["INTC", "AAPL", "AMD", "SPY", "NVDA", "QQQ", "LMT", "TSLA", "PLTR", "MSFT", "GOOGL"]

# Thresholds to consider an OI change "unusual"
# For example, any day-over-day OI jump by > 200% or more than +5,000 contracts
PERCENT_CHANGE_THRESHOLD = 200   # 200%
ABS_CHANGE_THRESHOLD = 5000      # 5,000 contracts

# Where we store the prior day's OI data
HISTORICAL_OI_FILE = "oi_baseline.csv"

# Your Discord Webhook URL
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1349931297492963419/AfFPe0MJwjPF4f4vVlzHk_FsfGccuwiBzWzJbqNJtIO4vXI1arjG0dbYtqVw1_eER28z"

# ---------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------

def load_previous_oi(filename):
    """
    Load previously stored OI data from a CSV into a DataFrame.
    Returns an empty DataFrame if file doesn't exist.
    """
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return pd.DataFrame(columns=["ticker","expiration","strike","type","openInterest"])

def save_current_oi(df, filename):
    """
    Save today's OI data as CSV for next time's comparison.
    """
    df.to_csv(filename, index=False)

def send_discord_alert(webhook_url, message):
    """
    Sends a message to a Discord channel via a webhook.
    """
    payload = {
        "content": message  # The text of your alert
    }
    response = requests.post(webhook_url, json=payload)
    if response.status_code not in (200, 204):
        print(f"Failed to send Discord message. Response: {response.text}")

# ---------------------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------------------

def main():
    # 1) Load the "yesterday" baseline
    prev_oi_df = load_previous_oi(HISTORICAL_OI_FILE)

    # Will gather records of today's OI
    current_oi_records = []

    # We'll store alert lines for any suspicious OI changes
    alert_lines = []

    for ticker in TICKERS:
        tk = yf.Ticker(ticker)

        # 2) Get the list of available option expirations from yfinance
        try:
            expirations = tk.options
        except Exception as e:
            print(f"[ERROR] Failed to fetch options for {ticker}: {e}")
            continue

        # 3) For each expiration, fetch the full option chain
        for expiry in expirations:
            try:
                opt_chain = tk.option_chain(expiry)
            except Exception as e:
                print(f"[ERROR] Could not fetch option chain for {ticker} {expiry}: {e}")
                continue

            # We'll combine calls and puts into one DataFrame, but keep track of type
            calls = opt_chain.calls.copy()
            calls["type"] = "call"
            puts = opt_chain.puts.copy()
            puts["type"] = "put"
            all_opts = pd.concat([calls, puts], ignore_index=True)
            all_opts["ticker"] = ticker
            all_opts["expiration"] = expiry

            # 4) Iterate over each option row to compare OI
            for idx, row in all_opts.iterrows():
                strike = row.get("strike", 0)
                opt_type = row.get("type", "")
                oi_today = row.get("openInterest", 0)

                # Record today's data
                current_oi_records.append({
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
                pct_change = 0
                if oi_yesterday != 0:
                    pct_change = (abs_change / oi_yesterday) * 100
                else:
                    # If yesterday's OI was zero but today's is > 0, treat as big jump
                    pct_change = 99999 if oi_today > 0 else 0

                # 5) Check if it meets our "unusual" thresholds
                if abs_change >= ABS_CHANGE_THRESHOLD or pct_change >= PERCENT_CHANGE_THRESHOLD:
                    line = (
                        f"**{ticker}** {expiry} {opt_type.upper()} {strike}\n"
                        f"OI changed from {oi_yesterday} to {oi_today} "
                        f"(+{abs_change}, +{pct_change:.1f}%)"
                    )
                    alert_lines.append(line)

    # 6) Turn our records into a DataFrame
    current_oi_df = pd.DataFrame(current_oi_records, columns=["ticker","expiration","strike","type","openInterest"])

    # 7) If we found anomalies, post them to Discord
    if alert_lines:
        alert_message = "UNUSUAL OI DETECTED:\n" + "\n".join(alert_lines)
        send_discord_alert(DISCORD_WEBHOOK_URL, alert_message)
        print("Sent Discord Alert:\n", alert_message)
    else:
        print("No unusual OI changes found.")

    # 8) Save today's OI as the baseline for the next run
    save_current_oi(current_oi_df, HISTORICAL_OI_FILE)

if __name__ == "__main__":
    main()