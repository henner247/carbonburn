import pandas as pd
import argparse

# CO2 emission factors (Mt CO2 per GWh)
EMISSION_FACTORS = {
    'hard_coal': 0.85,
    'lignite': 1.1,
    'gas': 0.4
}

def calculate_co2_emissions(input_csv, output_csv='eu_co2_daily.csv'):
    """
    Calculate daily CO2 emissions from generation data.
    
    Args:
        input_csv: Path to generation CSV (from generation_downloader.py)
        output_csv: Output CSV path
    
    Returns:
        DataFrame with CO2 emissions
    """
    print(f"Reading data from {input_csv}...")
    df = pd.read_csv(input_csv)
    
    # Aggregate by date (sum across all countries)
    daily_total = df.groupby('Date').agg({
        'Lignite_GWh': 'sum',
        'Hard_Coal_GWh': 'sum',
        'Gas_GWh': 'sum'
    }).reset_index()
    
    # Calculate CO2 emissions (Mt CO2)
    daily_total['CO2_Coal_Mt'] = daily_total['Hard_Coal_GWh'] * EMISSION_FACTORS['hard_coal']
    daily_total['CO2_Lignite_Mt'] = daily_total['Lignite_GWh'] * EMISSION_FACTORS['lignite']
    daily_total['CO2_Gas_Mt'] = daily_total['Gas_GWh'] * EMISSION_FACTORS['gas']
    daily_total['Total_CO2_Mt'] = (daily_total['CO2_Coal_Mt'] + 
                                    daily_total['CO2_Lignite_Mt'] + 
                                    daily_total['CO2_Gas_Mt'])
    
    # Keep only date and CO2 columns
    result = daily_total[['Date', 'CO2_Coal_Mt', 'CO2_Lignite_Mt', 'CO2_Gas_Mt', 'Total_CO2_Mt']]
    
    # Save to CSV
    result.to_csv(output_csv, index=False)
    print(f"\n[SUCCESS] Saved CO2 emissions to {output_csv}")
    print(f"\nSummary:")
    print(f"  Date range: {result['Date'].min()} to {result['Date'].max()}")
    print(f"  Total days: {len(result)}")
    print(f"\nTotal Emissions (Mt CO2):")
    print(f"  Coal:    {result['CO2_Coal_Mt'].sum():,.2f}")
    print(f"  Lignite: {result['CO2_Lignite_Mt'].sum():,.2f}")
    print(f"  Gas:     {result['CO2_Gas_Mt'].sum():,.2f}")
    print(f"  TOTAL:   {result['Total_CO2_Mt'].sum():,.2f}")
    print(f"\nSample data:")
    print(result.head(10))
    
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calculate CO2 emissions from EU generation data')
    parser.add_argument('--input', type=str, default='eu_generation_daily.csv', 
                        help='Input generation CSV file')
    parser.add_argument('--output', type=str, default='eu_co2_daily.csv', 
                        help='Output CO2 emissions CSV file')
    
    args = parser.parse_args()
    
    calculate_co2_emissions(args.input, args.output)
