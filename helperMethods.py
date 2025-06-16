import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from getBars import *


def getTradingDays(year):
    """
    Fetches a list of trading days for a specified year from the Alpaca API.
    
    Args:
        year (int): The year to get trading days for (e.g., 2025)
    
    Returns:
        list: A list of dates in YYYY-MM-DD format representing trading days
    
    Raises:
        Exception: If the API request fails or if no calendar data is available
    """
    config = load_config()
    
    # Set up the start and end dates for the full year
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    url = "https://paper-api.alpaca.markets/v2/calendar"
    
    params = {
        "start": start_date,
        "end": end_date
    }
    
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": config['ALPACA_KEY'],
        "APCA-API-SECRET-KEY": config['ALPACA_SECRET']
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}: {response.text}")
    
    calendar_data = response.json()
    
    if not calendar_data:
        raise Exception(f"No calendar data available for {year}")
    
    # Extract just the dates from the calendar data
    trading_days = [day['date'] for day in calendar_data]
    
    return trading_days

def simpleTestOfPrinciple(ticker, start_time=None, end_time=None):
    """
    Calculate overnight and intraday drift between 9:30 ET opens and 16:00 ET closes.
    Only processes trading days (excludes weekends and holidays).
    Handles early market closes by finding last price of each trading day.
    
    Overnight drift: Today's last price to tomorrow's 9:30 ET open
    Intraday drift: Today's 9:30 ET open to today's last price
    """
    # Get all bars
    df = get30MinuteBarAttributes(ticker, start_time, end_time)
    
    # Convert timestamps to datetime for easier handling
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Get trading days for the period
    start_year = df['timestamp'].dt.year.min()
    end_year = df['timestamp'].dt.year.max()
    trading_days = set()
    for year in range(start_year, end_year + 1):
        trading_days.update(getTradingDays(year))
    
    # Filter for trading days only
    df['date_str'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    df = df[df['date_str'].isin(trading_days)].copy()
    
    # Group by date to process each day
    df['date'] = df['timestamp'].dt.date
    dates = sorted(df['date'].unique())
    
    overnight_drifts = []
    intraday_drifts = []
    
    for i in range(len(dates)-1):  # Stop one day before the end
        today = dates[i]
        tomorrow = dates[i+1]
        
        try:
            # Get today's data
            today_df = df[df['date'] == today]
            
            # Get today's open (13:30 UTC / 9:30 ET)
            today_open_df = today_df[
                (today_df['timestamp'].dt.hour == 13) & 
                (today_df['timestamp'].dt.minute == 30)
            ]
            if len(today_open_df) == 0:
                print(f"Warning: No opening data for {today}")
                continue
            today_open = today_open_df['open'].iloc[0]
            
            # Get today's close (last bar of the day)
            today_close = today_df.iloc[-1]['close']
            
            # Get tomorrow's data
            tomorrow_df = df[df['date'] == tomorrow]
            
            # Get tomorrow's open (13:30 UTC / 9:30 ET)
            tomorrow_open_df = tomorrow_df[
                (tomorrow_df['timestamp'].dt.hour == 13) & 
                (tomorrow_df['timestamp'].dt.minute == 30)
            ]
            if len(tomorrow_open_df) == 0:
                print(f"Warning: No opening data for {tomorrow}")
                continue
            tomorrow_open = tomorrow_open_df['open'].iloc[0]
            
            # Calculate drifts
            overnight_drift = tomorrow_open - today_close
            intraday_drift = today_close - today_open
            
            # Store results
            overnight_drifts.append(overnight_drift)
            intraday_drifts.append(intraday_drift)
            
            # Optional: Print daily results for debugging
            # print(f"{today}: Intraday {intraday_drift:.2f}, Overnight {overnight_drift:.2f}")
            
        except Exception as e:
            print(f"Warning: Error processing {today}: {e}")
            continue
    
    if not overnight_drifts or not intraday_drifts:
        raise Exception("No valid drift periods found")
    
    # Calculate totals and averages
    overnight_total = sum(overnight_drifts)
    intraday_total = sum(intraday_drifts)
    
    # Print results
    print(f"\nResults:")
    print(f"Number of periods analyzed: {len(overnight_drifts)}")
    print(f"\nOvernight Drift (close → next 09:30 ET):")
    print(f"Total: {overnight_total:.2f}")
    print(f"Average: {overnight_total/len(overnight_drifts):.4f}")
    print(f"\nIntraday Drift (09:30 ET → close):")
    print(f"Total: {intraday_total:.2f}")
    print(f"Average: {intraday_total/len(intraday_drifts):.4f}")
    
    # Store additional metadata in dictionary
    results = {
        'periods': len(overnight_drifts),
        'overnight_total': overnight_total,
        'overnight_avg': overnight_total/len(overnight_drifts),
        'intraday_total': intraday_total,
        'intraday_avg': intraday_total/len(intraday_drifts),
        'overnight_drifts': overnight_drifts,
        'intraday_drifts': intraday_drifts,
        'dates': dates[:-1]  # Exclude last date since it's only used for next day's open
    }
    
    return results


# New method to return DataFrames of overnight and intraday drift indexed by date
def getDriftDataFrames(ticker, start_time=None, end_time=None):
    """
    Returns two DataFrames: one for overnight drift and one for intraday drift between trading sessions.
    Each DataFrame contains the drift values indexed by date.
    
    Args:
        ticker (str): The stock ticker symbol (e.g., "SPY")
        start_time (str or datetime, optional): Start date for the analysis
        end_time (str or datetime, optional): End date for the analysis

    Returns:
        list: [df_overnight, df_intraday]
    """
    df = get30MinuteBarAttributes(ticker, start_time, end_time)
    print(f"Initial bars DataFrame shape: {df.shape}")
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Determine trading days in the period
    start_year = df['timestamp'].dt.year.min()
    end_year = df['timestamp'].dt.year.max()
    trading_days = set()
    for year in range(start_year, end_year + 1):
        trading_days.update(getTradingDays(year))
    print(f"Date range: {start_year} to {end_year}, trading days count: {len(trading_days)}")

    df['date_str'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    df = df[df['date_str'].isin(trading_days)].copy()
    df['date'] = df['timestamp'].dt.date
    dates = sorted(df['date'].unique())

    overnight_drifts = []
    intraday_drifts = []

    for i in range(len(dates) - 1):
        today = dates[i]
        tomorrow = dates[i + 1]
        print(f"Processing date: {today}")

        try:
            today_df = df[df['date'] == today]
            tomorrow_df = df[df['date'] == tomorrow]

            today_open_df = today_df[
                (today_df['timestamp'].dt.hour == 13) & 
                (today_df['timestamp'].dt.minute == 30)
            ]
            if today_open_df.empty:
                print(f"Warning: No opening data for {today}")
                continue
            today_open = today_open_df['open'].iloc[0]

            today_close_df = today_df[
                (today_df['timestamp'].dt.hour == 19) & 
                (today_df['timestamp'].dt.minute == 30)
            ]
            if today_close_df.empty:
                print(f"Warning: No close bar found for {today}")
                continue
            today_close = today_close_df['close'].iloc[0]

            tomorrow_open_df = tomorrow_df[
                (tomorrow_df['timestamp'].dt.hour == 13) & 
                (tomorrow_df['timestamp'].dt.minute == 30)
            ]
            if tomorrow_open_df.empty:
                print(f"Warning: No opening data for {tomorrow}")
                continue
            tomorrow_open = tomorrow_open_df['open'].iloc[0]

            print(f"today_open: {today_open}, today_close: {today_close}, tomorrow_open: {tomorrow_open}")

            overnight_drift = tomorrow_open - today_close
            intraday_drift = today_close - today_open

            print(f"overnight_drift: {overnight_drift}, intraday_drift: {intraday_drift}")

            overnight_drifts.append({'date': today, 'overnight_drift': overnight_drift, 'close': today_close, 'pct_chg': overnight_drift / today_close * 100})
            intraday_drifts.append({'date': today, 'intraday_drift': intraday_drift, 'close': today_close, 'pct_chg': intraday_drift / today_open * 100})

        except Exception as e:
            print(f"Warning: Error processing {today}: {e}")
            continue

    df_overnight = pd.DataFrame(overnight_drifts)
    df_intraday = pd.DataFrame(intraday_drifts)

    print("Overnight drift DataFrame preview:")
    print(df_overnight.head())
    print("Intraday drift DataFrame preview:")
    print(df_intraday.head())

    return [df_intraday, df_overnight]

def get30MinDriftPerBar(ticker, start_time, end_time):
    """
    Computes per-bar drift (delta = close - open) for 30-minute bars, and returns intraday and overnight bars
    with additional columns: 'delta', 'timestamp_str', and 'type' ('intraday' or 'overnight').
    Args:
        ticker (str): Stock ticker symbol.
        start_time (str): Start time in ISO format.
        end_time (str): End time in ISO format.
    Returns:
        tuple: (intraday_df, overnight_df) with additional columns.
    """
    df = get30MinuteBarAttributes(ticker, start_time, end_time)
    
    # Convert timestamp to datetime with UTC timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    
    # Convert to America/New_York timezone
    df['timestamp'] = df['timestamp'].dt.tz_convert('America/New_York')
    
    # Add ISO-formatted string column
    df['timestamp_str'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # Split into intraday and overnight
    intraday_df = getIntradayBarAttributes(df.copy())
    overnight_df = getOvernightBarAttributes(df.copy())
    
    # Assign type
    intraday_df['type'] = 'intraday'
    overnight_df['type'] = 'overnight'
    
    # Calculate delta normally for intraday
    intraday_df['delta'] = intraday_df['close'] - intraday_df['open']
    
    # Calculate delta for overnight
    overnight_df['delta'] = 0.0
    overnight_df = overnight_df.reset_index(drop=True)
    for i in range(len(overnight_df)):
        curr_row = overnight_df.loc[i]
        if i > 0:
            prev_close = overnight_df.loc[i - 1, 'close']
        else:
            prev_close = curr_row['open']  # fallback for first row

        curr_time = curr_row['timestamp'].time()

        # Only compute close - prev_close for the first bar after 23:30 UTC (i.e., 08:00 UTC bar)
        if curr_time == pd.to_datetime("08:00:00").time():
            overnight_df.loc[i, 'delta'] = curr_row['close'] - prev_close
        else:
            overnight_df.loc[i, 'delta'] = curr_row['close'] - curr_row['open']
    
    # Recompute timestamp_str for updated DFs
    intraday_df['timestamp_str'] = intraday_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    overnight_df['timestamp_str'] = overnight_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    return [intraday_df, overnight_df]

def load_continuous_dataframes(ticker,start_time, end_time):
    overnightdf = get30MinDriftPerBar(
        ticker=ticker,
        start_time=start_time,
        end_time=end_time,
    )[1]
    intradaydf = get30MinDriftPerBar(
        ticker=ticker,
        start_time=start_time,
        end_time=end_time,
    )[0]

    overnightdf['pct_chg'] = overnightdf['delta'] / overnightdf['close'] * 100
    intradaydf['pct_chg'] = intradaydf['delta'] / intradaydf['open'] * 100

    overnightdf['cumulative_growth'] = 100.0
    for i in range(1, len(overnightdf)):
        overnightdf.loc[i, 'cumulative_growth'] = overnightdf.loc[i - 1, 'cumulative_growth'] * 0.01* (100 + overnightdf.loc[i, 'pct_chg'])

    intradaydf['cumulative_growth'] = 100.0
    for i in range(1, len(intradaydf)):
        intradaydf.loc[i, 'cumulative_growth'] = intradaydf.loc[i - 1, 'cumulative_growth'] * 0.01 * (100 + intradaydf.loc[i, 'pct_chg'])

    return [intradaydf, overnightdf]

def plot_cumulative_growth(intraday_df, overnight_df):
    plt.figure(figsize=(14, 6))
    
    # Plot intraday
    plt.plot(intraday_df['timestamp'], intraday_df['cumulative_growth'], label='Intraday', linewidth=2)
    
    # Plot overnight
    plt.plot(overnight_df['timestamp'], overnight_df['cumulative_growth'], label='Overnight', linewidth=2)
    
    # Formatting
    plt.title('Cumulative Growth Over Time')
    plt.xlabel('Timestamp')
    plt.ylabel('Cumulative Growth (Indexed at 100)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.xticks(rotation=45)
    plt.show()

def getReturnsByHour(df):
    """
    Calculate returns by hour for a DataFrame with 30-minute bars.
    
    Args:
        df (DataFrame): DataFrame with 'timestamp', 'open', and 'close' columns.
    
    Returns:
        DataFrame: Returns by hour with 'hour' and 'return' columns.
    """
    df['hour'] = df['timestamp'].dt.hour
    df['return'] = (df['close'] - df['open']) / df['open']*100
    
    return df.groupby('hour')['return'].mean().reset_index()


# Plot daily percentage change for intraday and overnight drift
def plot_daily_pct_change(intraday_df, overnight_df):
    """
    Plots daily percentage change for intraday and overnight drift.

    Args:
        intraday_df (DataFrame): DataFrame with 'date' and 'pct_chg' columns for intraday drift.
        overnight_df (DataFrame): DataFrame with 'date' and 'pct_chg' columns for overnight drift.
    """
    plt.figure(figsize=(14, 6))
    
    # Plot intraday daily % change
    plt.plot(intraday_df['date'], intraday_df['pct_chg'], label='Intraday % Change', alpha=0.7)

    # Plot overnight daily % change
    plt.plot(overnight_df['date'], overnight_df['pct_chg'], label='Overnight % Change', alpha=0.7)

    plt.title('Daily Percentage Change: Intraday vs. Overnight')
    plt.xlabel('Date')
    plt.ylabel('Percentage Change')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.xticks(rotation=45)
    plt.show()


# New: Plot cumulative daily drift for intraday and overnight
def plot_cumulative_daily_drift(ticker, intraday_df, overnight_df):
    """
    Plots cumulative growth over time for intraday and overnight drift using daily percentage changes.

    Args:
        intraday_df (DataFrame): DataFrame with 'date' and 'pct_chg' columns for intraday drift.
        overnight_df (DataFrame): DataFrame with 'date' and 'pct_chg' columns for overnight drift.
    """
    # Copy to avoid modifying original data
    intraday = intraday_df[['date', 'pct_chg']].copy()
    overnight = overnight_df[['date', 'pct_chg']].copy()

    # Sort by date
    intraday.sort_values('date', inplace=True)
    overnight.sort_values('date', inplace=True)

    # Calculate cumulative growth
    intraday['cumulative'] = 100.0
    overnight['cumulative'] = 100.0

    for i in range(1, len(intraday)):
        intraday.loc[i, 'cumulative'] = intraday.loc[i - 1, 'cumulative'] * (1 + intraday.loc[i, 'pct_chg'] / 100.0)

    for i in range(1, len(overnight)):
        overnight.loc[i, 'cumulative'] = overnight.loc[i - 1, 'cumulative'] * (1 + overnight.loc[i, 'pct_chg'] / 100.0)

    # Plot
    plt.figure(figsize=(14, 6))
    plt.plot(intraday['date'], intraday['cumulative'], label='Intraday Cumulative', linewidth=2)
    plt.plot(overnight['date'], overnight['cumulative'], label='Overnight Cumulative', linewidth=2)

    plt.title('Cumulative Drift Over Time: Intraday vs. Overnight for ' + ticker)
    plt.xlabel('Date')
    plt.ylabel('Cumulative Growth (Indexed at 100)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.xticks(rotation=45)
    plt.show()