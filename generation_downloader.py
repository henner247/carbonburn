import requests
import pandas as pd
from datetime import datetime, timedelta
import argparse
import os

# Country codes for Energy Charts API
COUNTRIES = {
    'Germany': 'de',
    'France': 'fr',
    'Italy': 'it',
    'Spain': 'es',
    'Poland': 'pl',
    'Netherlands': 'nl',
    'Belgium': 'be',
    'Sweden': 'se',
    'Austria': 'at',
    'Czech Republic': 'cz',
    'Romania': 'ro'
}

# Fuel type mappings
FUEL_TYPES = {
    'lignite': 'Fossil brown coal / lignite',
    'hard_coal': 'Fossil hard coal',
    'gas': 'Fossil gas'
}

def download_generation(country_code, start_date, end_date):
    """
    Download generation data for a country from Energy Charts API.
    """
    url = "https://api.energy-charts.info/public_power"
    params = {
        'country': country_code,
        'start': start_date,
        'end': end_date
    }
    
    print(f"Downloading data for {country_code} from {start_date} to {end_date}...")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Parse the response
        if not data or 'unix_seconds' not in data:
            print(f"Warning: No data returned for {country_code}")
            return None
            
        timestamps = data['unix_seconds']
        production_types = data.get('production_types', [])
        
        # Convert timestamps to datetime
        dates = [datetime.fromtimestamp(ts) for ts in timestamps]
        
        # Extract generation by fuel type
        result = {'timestamp': dates}
        
        for ptype in production_types:
            fuel_name = ptype.get('name')
            if fuel_name in FUEL_TYPES.values():
                values = ptype.get('data', [])
                # Map fuel type to simplified name
                for key, val in FUEL_TYPES.items():
                    if val == fuel_name:
                        result[key] = values
                        break
        
        df = pd.DataFrame(result)
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data for {country_code}: {e}")
        return None

def aggregate_daily(df, country_name):
    """
    Aggregate hourly data to daily sums.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    df['date'] = df['timestamp'].dt.date
    
    # Ensure all fuel types exist (default to 0 if missing)
    for fuel_type in ['lignite', 'hard_coal', 'gas']:
        if fuel_type not in df.columns:
            df[fuel_type] = 0.0
            
    # Calculate energy (MWh) from power (MW)
    if len(df) > 1:
        df = df.sort_values('timestamp')
        intervals = df['timestamp'].diff().dt.total_seconds() / 3600.0
        intervals.iloc[0] = intervals.iloc[1]
        intervals = intervals.clip(upper=1.0)
        
        for fuel in ['lignite', 'hard_coal', 'gas']:
            df[fuel] = df[fuel] * intervals
            
        print(f"  [INFO] Processed {len(df)} points with robust interval detection (max 1h)")
    else:
        for fuel in ['lignite', 'hard_coal', 'gas']:
            df[fuel] = df[fuel] * 1.0
    
    daily = df.groupby('date').agg({
        'lignite': 'sum',
        'hard_coal': 'sum',
        'gas': 'sum'
    }).reset_index()
    
    daily['Lignite_GWh'] = daily['lignite'] / 1000
    daily['Hard_Coal_GWh'] = daily['hard_coal'] / 1000
    daily['Gas_GWh'] = daily['gas'] / 1000
    daily['Total_Fossil_GWh'] = daily['Lignite_GWh'] + daily['Hard_Coal_GWh'] + daily['Gas_GWh']
    
    daily['Country'] = country_name
    daily = daily[['date', 'Country', 'Lignite_GWh', 'Hard_Coal_GWh', 'Gas_GWh', 'Total_Fossil_GWh']]
    daily.columns = ['Date', 'Country', 'Lignite_GWh', 'Hard_Coal_GWh', 'Gas_GWh', 'Total_Fossil_GWh']
    
    return daily

def get_latest_date_from_csv(file_path):
    """Find the latest date in the existing CSV file."""
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            return df['Date'].max()
    except Exception as e:
        print(f"Warning: Could not read existing CSV: {e}")
    return None

def download_all_countries(start_date, end_date, output_file='eu_generation_daily.csv'):
    """
    Download and aggregate data for all countries.
    """
    all_data = []
    
    # Load existing data if it exists
    existing_df = None
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            existing_df['Date'] = pd.to_datetime(existing_df['Date'])
        except:
            pass
            
    for country_name, country_code in COUNTRIES.items():
        print(f"\nProcessing {country_name}...")
        
        # Download hourly data
        hourly_df = download_generation(country_code, start_date, end_date)
        
        if hourly_df is not None and not hourly_df.empty:
            # Aggregate to daily
            daily_df = aggregate_daily(hourly_df, country_name)
            all_data.append(daily_df)
            print(f"  [OK] Downloaded {len(daily_df)} days of data")
        else:
            print(f"  [SKIP] No data available")
    
    if all_data:
        # Combine all new data
        combined_new = pd.concat(all_data, ignore_index=True)
        combined_new['Date'] = pd.to_datetime(combined_new['Date'])
        
        if existing_df is not None:
            # Merge and deduplicate
            final_df = pd.concat([existing_df, combined_new], ignore_index=True)
            # Remove exact duplicates (Date + Country)
            final_df = final_df.drop_duplicates(subset=['Date', 'Country'], keep='last')
        else:
            final_df = combined_new
            
        final_df = final_df.sort_values(['Date', 'Country'])
        
        # Save to CSV
        final_df.to_csv(output_file, index=False)
        print(f"\n[SUCCESS] Saved {len(final_df)} total rows to {output_file}")
        print(f"\nSummary:")
        print(f"  Date range: {final_df['Date'].min().date()} to {final_df['Date'].max().date()}")
        print(f"  Countries: {final_df['Country'].nunique()}")
        
        return final_df
    else:
        print("\n[INFO] No new data was added.")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download EU fossil generation data')
    parser.add_argument('--start', type=str, default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None, help='End date (YYYY-MM-DD), defaults to yesterday')
    parser.add_argument('--output', type=str, default='eu_generation_daily.csv', help='Output CSV file')
    parser.add_argument('--incremental', action='store_true', help='Download only missing data since last record')
    
    args = parser.parse_args()
    
    start_date = args.start
    if args.incremental or (start_date is None and os.path.exists(args.output)):
        latest = get_latest_date_from_csv(args.output)
        if latest:
            # Start from the latest date we have
            start_date = latest.strftime('%Y-%m-%d')
            print(f"Incremental update: detected latest date {start_date} in {args.output}")
    
    # Use default start if still None
    if start_date is None:
        start_date = '2023-01-01'
        
    # Default end date to yesterday if not specified
    if args.end is None:
        args.end = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # If start is after or same as end (except for today's check), we might already be up to date
    if start_date > args.end:
        print(f"Data is already up to date (up to {start_date}).")
    else:
        new_data = download_all_countries(start_date, args.end, args.output)
