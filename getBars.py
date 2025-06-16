import json
import requests
import pandas as pd
from datetime import datetime, timedelta

def load_config():
    with open('config.json', 'r') as f:
        # Load the JSON config file (with comments supported)
        config = json.load(f)
    return config

def get5MinuteBarAttributes(ticker, start_time=None, end_time=None):
    """
    Fetches the most recent 5-minute bar attributes for a given stock ticker from the Alpaca API.
    Args:
        ticker (str): The stock ticker symbol to retrieve bar data for.
        start_time (datetime or str, optional): The start time for the bar data. Can be a datetime object or an ISO 8601 string. 
        end_time (datetime or str, optional): The end time for the bar data. If not provided, it defaults to 5 minutes after start_time.
        Preferred format is "YYYY-MM-DDTHH:mm:ss+{utc offset, -4 for eastern standard time}".
            If not provided, defaults to 10 minutes before the current time to ensure retrieval of the last complete 5-minute bar.
    Returns:
        list: A list containing the following attributes of the latest 5-minute bar:
            [open, high, low, close, volume, timestamp]
            open: The opening price of the bar.
            high: The highest price of the bar.
            low: The lowest price of the bar.
            close: The closing price of the bar.
            volume: The trading volume during the bar.
            timestamp: The timestamp of the bar in ISO 8601 format.

    Raises:
        Exception: If the API request fails or if no bar data is available for the specified ticker.
    """
    config = load_config()
    
    url = "https://data.alpaca.markets/v2/stocks/bars"
    
   # Handle start_time
    if start_time is None:
        start_time = datetime.now() - timedelta(minutes=10)
    elif isinstance(start_time, str):
        time_part, offset = start_time.rsplit('-', 1)
        if ':' in offset:
            hours, minutes = map(int, offset.split(':'))
            offset_minutes = hours * 60 + minutes
        else:
            offset_minutes = int(offset) * 60
        start_time = datetime.fromisoformat(time_part)
        start_time = start_time + timedelta(minutes=offset_minutes)

    # Handle end_time
    if end_time is None:
        end_time = start_time + timedelta(minutes=5)
    elif isinstance(end_time, str):
        time_part, offset = end_time.rsplit('-', 1)
        if ':' in offset:
            hours, minutes = map(int, offset.split(':'))
            offset_minutes = hours * 60 + minutes
        else:
            offset_minutes = int(offset) * 60
        end_time = datetime.fromisoformat(time_part)
        end_time = end_time + timedelta(minutes=offset_minutes)
    
    params = {
        "symbols": ticker,
        "timeframe": "5Min",
        "start": start_time.strftime("%Y-%m-%dT%H:%M:00Z"),  # Start at exact minute, no microseconds
        "end": end_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "limit": 10000,
        "adjustment": "raw",
        "feed": "sip",
        "sort": "asc"  # Get earliest quote first (time series data)
    }
    
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": config['ALPACA_KEY'],
        "APCA-API-SECRET-KEY": config['ALPACA_SECRET']
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}: {response.text}")
    
    data = response.json()
    
    if not data.get('bars') or not data['bars'].get(ticker) or len(data['bars'][ticker]) == 0:
        raise Exception(f"No bar data available for {ticker}")
    
    # Convert to DataFrame
    bars = data['bars'][ticker]
    df = pd.DataFrame(bars)
    
    # Rename columns to match previous format
    df = df.rename(columns={
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume',
        't': 'timestamp'
    })
    
    # Reorder columns to match previous format
    df = df[['open', 'high', 'low', 'close', 'volume', 'timestamp']]
    
    return df

def get15MinuteBarAttributes(ticker, start_time=None, end_time=None):
    """
    Fetches 15-minute bar attributes for a given stock ticker from the Alpaca API.
    Args:
        ticker (str): The stock ticker symbol to retrieve bar data for.
        start_time (datetime or str, optional): The start time for the bar data.
        end_time (datetime or str, optional): The end time for the bar data.
        Format: "YYYY-MM-DDTHH:mm:ss-04:00" (Eastern time)
    Returns:
        pandas.DataFrame: DataFrame with columns [open, high, low, close, volume, timestamp]
    """
    config = load_config()
    
    url = "https://data.alpaca.markets/v2/stocks/bars"
    
   # Handle start_time
    if start_time is None:
        start_time = datetime.now() - timedelta(minutes=10)
    elif isinstance(start_time, str):
        time_part, offset = start_time.rsplit('-', 1)
        if ':' in offset:
            hours, minutes = map(int, offset.split(':'))
            offset_minutes = hours * 60 + minutes
        else:
            offset_minutes = int(offset) * 60
        start_time = datetime.fromisoformat(time_part)
        start_time = start_time + timedelta(minutes=offset_minutes)

    # Handle end_time
    if end_time is None:
        end_time = start_time + timedelta(minutes=5)
    elif isinstance(end_time, str):
        time_part, offset = end_time.rsplit('-', 1)
        if ':' in offset:
            hours, minutes = map(int, offset.split(':'))
            offset_minutes = hours * 60 + minutes
        else:
            offset_minutes = int(offset) * 60
        end_time = datetime.fromisoformat(time_part)
        end_time = end_time + timedelta(minutes=offset_minutes)
    
    params = {
        "symbols": ticker,
        "timeframe": "15Min",
        "start": start_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "end": end_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "limit": 10000,
        "adjustment": "raw",
        "feed": "sip",
        "sort": "asc"
    }
     
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": config['ALPACA_KEY'],
        "APCA-API-SECRET-KEY": config['ALPACA_SECRET']
    }

    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}: {response.text}")
    
    data = response.json()
    
    if not data.get('bars') or not data['bars'].get(ticker) or len(data['bars'][ticker]) == 0:
        raise Exception(f"No bar data available for {ticker}")
    
    # Convert to DataFrame
    bars = data['bars'][ticker]
    df = pd.DataFrame(bars)
    
    # Rename columns to match previous format
    df = df.rename(columns={
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume',
        't': 'timestamp'
    })
    
    # Reorder columns to match previous format
    df = df[['open', 'high', 'low', 'close', 'volume', 'timestamp']]
    
    return df

def get30MinuteBarAttributes(ticker, start_time=None, end_time=None):
    """
    Fetches 30-minute bar attributes for a given stock ticker from the Alpaca API.
    Handles pagination to get all available bars in the time range.
    
    Args:
        ticker (str): The stock ticker symbol to retrieve bar data for.
        start_time (datetime or str, optional): The start time for the bar data.
        end_time (datetime or str, optional): The end time for the bar data.
        Preferred format is "YYYY-MM-DDTHH:mm:ss-04:00" (Eastern time).
    
    Returns:
        pandas.DataFrame: DataFrame containing all bar data with columns:
            [open, high, low, close, volume, timestamp]
    """
    config = load_config()
    url = "https://data.alpaca.markets/v2/stocks/bars"
    all_bars = []

    # Handle start_time
    if start_time is None:
        start_time = datetime.now() - timedelta(minutes=10)
    elif isinstance(start_time, str):
        time_part, offset = start_time.rsplit('-', 1)
        if ':' in offset:
            hours, minutes = map(int, offset.split(':'))
            offset_minutes = hours * 60 + minutes
        else:
            offset_minutes = int(offset) * 60
        start_time = datetime.fromisoformat(time_part)
        start_time = start_time + timedelta(minutes=offset_minutes)

    # Handle end_time
    if end_time is None:
        end_time = start_time + timedelta(minutes=30)
    elif isinstance(end_time, str):
        time_part, offset = end_time.rsplit('-', 1)
        if ':' in offset:
            hours, minutes = map(int, offset.split(':'))
            offset_minutes = hours * 60 + minutes
        else:
            offset_minutes = int(offset) * 60
        end_time = datetime.fromisoformat(time_part)
        end_time = end_time + timedelta(minutes=offset_minutes)
    
    # Initial parameters
    params = {
        "symbols": ticker,  # Ensure ticker is passed as a string, not a list
        "timeframe": "30Min",
        "start": start_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "end": end_time.strftime("%Y-%m-%dT%H:%M:00Z"),
        "adjustment": "raw",
        "feed": "sip",
        "sort": "asc"
    }
     
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": config['ALPACA_KEY'],
        "APCA-API-SECRET-KEY": config['ALPACA_SECRET']
    }

    # Keep fetching while there are more pages
    while True:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
        
        data = response.json()
        
        if not data.get('bars') or not data['bars'].get(ticker):
            if not all_bars:  # If this is the first request and no data
                raise Exception(f"No bar data available for {ticker}")
            break  # If we've already got some bars, just finish
            
        # Add this page's bars to our collection
        all_bars.extend(data['bars'][ticker])
        
        # Check if there's a next page
        if not data.get('next_page_token'):
            break
            
        # Update params with the page token for next iteration
        params['page_token'] = data['next_page_token']
    
    if not all_bars:
        raise Exception(f"No bar data available for {ticker}")
    
    # Convert all collected bars to DataFrame
    df = pd.DataFrame(all_bars)
    
    # Rename columns to match previous format
    df = df.rename(columns={
        'o': 'open',
        'h': 'high',
        'l': 'low',
        'c': 'close',
        'v': 'volume',
        't': 'timestamp'
    })
    
    # Reorder columns to match previous format
    df = df[['open', 'high', 'low', 'close', 'volume', 'timestamp']]
    
    return df

def getIntradayBarAttributes(df):
    """
    Filters a DataFrame to only include intraday bars (9:30 AM - 4:00 PM Eastern).
    
    Args:
        df (pandas.DataFrame): DataFrame with bar data containing 'timestamp' column
            in ISO 8601 UTC format (e.g., "2025-06-06T13:30:00Z")
    
    Returns:
        pandas.DataFrame: Filtered DataFrame with only intraday bars, indexed from 0
    """
    # Convert UTC timestamp strings to datetime objects with UTC timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)
    
    # Convert UTC to Eastern time
    eastern = df['timestamp'].dt.tz_convert('America/New_York')
    
    # Create mask for trading hours (9:30 AM - 4:00 PM Eastern)
    mask = (
        ((eastern.dt.hour == 9) & (eastern.dt.minute >= 30)) |  # After 9:30 AM
        ((eastern.dt.hour > 9) & (eastern.dt.hour < 16))      # 10 AM - 3:59 PM
    )
    
    # Apply filter and reset index
    df = df[mask].copy().reset_index(drop=True)
    
    # Ensure columns are in correct order
    df = df[['open', 'high', 'low', 'close', 'volume', 'timestamp']]
    
    return df

def getOvernightBarAttributes(df):
    """
    Filters a DataFrame to only include overnight bars (4:00 PM - 9:30 AM Eastern next day).
    
    Args:
        df (pandas.DataFrame): DataFrame with bar data containing 'timestamp' column
            in ISO 8601 UTC format (e.g., "2025-06-06T13:30:00Z")
    
    Returns:
        pandas.DataFrame: Filtered DataFrame with only overnight bars, indexed from 0
    """
    # Convert UTC timestamp strings to datetime objects with UTC timezone
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)
    
    # Convert UTC to Eastern time
    eastern = df['timestamp'].dt.tz_convert('America/New_York')
    
    # Create mask for overnight hours (4:00 PM - 9:30 AM Eastern)
    mask = (
        ((eastern.dt.hour >= 16)) |                              #Bar begins at or after 4 PM
        (eastern.dt.hour < 9) |                              # Before 9 AM
        (eastern.dt.hour == 9) & (eastern.dt.minute < 30)  # Before 9:30 AM
    )
    
    # Apply filter and reset index
    df = df[mask].copy().reset_index(drop=True)
    
    # Ensure columns are in correct order
    df = df[['open', 'high', 'low', 'close', 'volume', 'timestamp']]
    
    return df

