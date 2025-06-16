import json
import requests
from datetime import datetime, timedelta
from getBars import *
from helperMethods import *

def load_config():
    with open('config.json', 'r') as f:
        # Load the JSON config file (with comments supported)
        config = json.load(f)
    return config

# would be curious to see overnight drift BUT WHEN FUTURES ARE CLOSED



def generate_year_ranges(start_year, end_year):
    ranges = []
    for year in range(start_year, end_year + 1):
        start = f"{year}-01-01T00:00:00-04:00"
        end = f"{year}-12-31T23:59:59-04:00"
        ranges.append((start, end))
    return ranges


if __name__ == "__main__":
    config = load_config()
    
    try:
        ticker = "SPY"
        start_year = 2017
        end_year = 2024

        all_intraday_dfs = []
        all_overnight_dfs = []

        for start_time, end_time in generate_year_ranges(start_year, end_year):
            try:
                intraday_df, overnight_df = getDriftDataFrames(ticker, start_time, end_time)
                all_intraday_dfs.append(intraday_df)
                all_overnight_dfs.append(overnight_df)
                print(f"Processed {start_time} to {end_time}")
            except Exception as e:
                print(f"Failed {start_time} to {end_time}: {e}")
                continue

        # Concatenate all results
        full_intraday = pd.concat(all_intraday_dfs, ignore_index=True)
        full_overnight = pd.concat(all_overnight_dfs, ignore_index=True)

        # Print average drift
        print("\n=== Combined Results ===")
        print(f"Intraday Mean Drift: {full_intraday['pct_chg'].mean():.6f}")
        print(f"Overnight Mean Drift: {full_overnight['pct_chg'].mean():.6f}")

        # Optionally plot
        plot_cumulative_daily_drift(ticker, full_intraday, full_overnight)

    except Exception as e:
        print(f"Error: {e}")