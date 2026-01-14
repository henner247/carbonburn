import pandas as pd

def analyze():
    print("Loading coverage_check_2023.csv...")
    try:
        df = pd.read_csv('coverage_check_2023.csv')
    except FileNotFoundError:
        print("Error: coverge_check_2023.csv not found.")
        return

    # Filter for 2023 only (just to be safe, though we requested 2023)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[df['Date'].dt.year == 2023]

    if df.empty:
        print("No data for 2023 found.")
        return

    # Calculate Sums
    total_lignite = df['Lignite_GWh'].sum() / 1000.0 # TWh
    total_hard_coal = df['Hard_Coal_GWh'].sum() / 1000.0 # TWh
    total_gas = df['Gas_GWh'].sum() / 1000.0 # TWh

    total_coal = total_lignite + total_hard_coal

    print(f"--- 2023 Generation for Tracked Countries ---")
    print(f"Lignite:   {total_lignite:.2f} TWh")
    print(f"Hard Coal: {total_hard_coal:.2f} TWh")
    print(f"Total Coal:{total_coal:.2f} TWh")
    print(f"Gas:       {total_gas:.2f} TWh")
    print("-" * 40)

    # Comparison with Ember 2023 EU Totals
    # Sources: https://ember-climate.org/insights/research/european-electricity-review-2024/
    # Coal (Hard + Brown): 333 TWh
    # Gas: 452 TWh
    
    ember_coal = 333.0
    ember_gas = 452.0

    coal_cvg = (total_coal / ember_coal) * 100
    gas_cvg = (total_gas / ember_gas) * 100

    print(f"--- Coverage Estimate (vs Ember EU-27) ---")
    print(f"Coal Coverage: {coal_cvg:.1f}% ({total_coal:.1f}/{ember_coal} TWh)")
    print(f"Gas Coverage:  {gas_cvg:.1f}% ({total_gas:.1f}/{ember_gas} TWh)")

if __name__ == "__main__":
    analyze()
