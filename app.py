import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import subprocess
import os

# Page configuration
st.set_page_config(page_title="EU CO2 Emissions Dashboard", layout="wide")

st.title("🌍 EU Fossil Fuel CO2 Emissions Dashboard")
st.markdown("""
This dashboard analyzes CO2 emissions from fossil fuel power generation across 11 major EU countries.
All values are shown in **Million Tonnes (Mt)**.
""")

# Sidebar for controls
st.sidebar.header("Controls")

import sys

def refresh_data():
    with st.spinner("Downloading new data and recalculating..."):
        try:
            # Run downloader with incremental flag
            result_down = subprocess.run([sys.executable, "generation_downloader.py", "--incremental"], capture_output=True, text=True, check=True)
            # Run calculator
            result_calc = subprocess.run([sys.executable, "co2_calculator.py"], capture_output=True, text=True, check=True)
            
            st.sidebar.success("Data updated successfully!")
            
        except subprocess.CalledProcessError as e:
            st.sidebar.error("An error occurred while updating data.")
            with st.sidebar.expander("Error Details"):
                st.code(f"Command: {e.cmd}\nReturn Code: {e.returncode}")
                if e.stdout:
                    st.text("Output:")
                    st.code(e.stdout)
                if e.stderr:
                    st.text("Error Output:")
                    st.code(e.stderr)
        except Exception as e:
            st.sidebar.error(f"Unexpected error: {str(e)}")

if st.sidebar.button("🔄 Refresh Data"):
    refresh_data()

# Load Data
@st.cache_data(ttl=3600)
def load_data():
    if not os.path.exists('eu_co2_daily.csv'):
        return None
    df = pd.read_csv('eu_co2_daily.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Filter for years since 2023
    df = df[df['Date'] >= '2023-01-01'].copy()
    
    # Values in CSV are in kt, convert to Mt
    df['Total_CO2_Mt'] = df['Total_CO2_Mt'] / 1000
    
    # Add grouping columns
    df['Year'] = df['Date'].dt.year
    df['DayOfYear'] = df['Date'].dt.dayofyear
    return df

df = load_data()

if df is None:
    st.error("Data file not found. Please click 'Refresh Data' in the sidebar.")
else:
    # --- Data Calculations ---
    most_recent_date = df['Date'].max()
    last_14_days_start = most_recent_date - timedelta(days=13)
    
    # 1. Cumulative Data
    df_pivot = df.pivot(index='DayOfYear', columns='Year', values='Total_CO2_Mt')
    df_cumulative = df_pivot.cumsum()
    
    # 2. 14-day Window Comparison (Same Period)
    current_day_of_year = most_recent_date.timetuple().tm_yday
    start_day_of_year = last_14_days_start.timetuple().tm_yday
    
    yearly_window_stats = []
    for year in sorted(df['Year'].unique()):
        year_data = df[df['Year'] == year].copy()
        
        # Handle wrap around if needed (though usually Dec-Jan)
        if start_day_of_year <= current_day_of_year:
            period_data = year_data[(year_data['DayOfYear'] >= start_day_of_year) & 
                                    (year_data['DayOfYear'] <= current_day_of_year)]
        else:
            period_data = year_data[(year_data['DayOfYear'] >= start_day_of_year) | 
                                    (year_data['DayOfYear'] <= current_day_of_year)]
        
        if not period_data.empty:
            yearly_window_stats.append({
                'Year': str(year),
                'Total_CO2_Sum': period_data['Total_CO2_Mt'].sum(),
                'Avg_CO2_Daily': period_data['Total_CO2_Mt'].mean(),
                'Days': len(period_data)
            })
    
    window_df = pd.DataFrame(yearly_window_stats)

    # --- Layout ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Latest Date", most_recent_date.strftime('%Y-%m-%d'))
    with col2:
        current_daily_avg = window_df.iloc[-1]['Avg_CO2_Daily'] if not window_df.empty else 0
        st.metric("Recent Daily Avg (Mt)", f"{current_daily_avg:.2f}")
    with col3:
        total_countries = 11
        st.metric("Countries", total_countries)

    st.divider()

    # --- Chart 1: Cumulative CO2 ---
    st.subheader("📈 Cumulative CO2 Emissions (Year-to-Date)")
    fig_cum = go.Figure()
    for year in df_cumulative.columns:
        fig_cum.add_trace(go.Scatter(
            x=df_cumulative.index, 
            y=df_cumulative[year],
            mode='lines',
            name=str(year),
            hovertemplate='Day %{x}<br>%{y:.2f} Mt'
        ))
    
    fig_cum.update_layout(
        xaxis_title="Day of Year",
        yaxis_title="Cumulative CO2 (Million Tonnes)",
        hovermode="x unified",
        legend_title="Year",
        height=600,
        margin=dict(l=0, r=0, t=30, b=0)
    )
    st.plotly_chart(fig_cum, use_container_width=True)

    # --- Charts 2 & 3: Bar Comparisons ---
    c_bar1, c_bar2 = st.columns(2)
    
    with c_bar1:
        st.subheader(f"📊 Total 14-Day Sum ({last_14_days_start.strftime('%b %d')} - {most_recent_date.strftime('%b %d')})")
        fig_sum = px.bar(window_df, x='Year', y='Total_CO2_Sum', 
                         color='Year', text_auto='.2f',
                         labels={'Total_CO2_Sum': 'Total CO2 (Mt)'})
        fig_sum.update_traces(textposition='outside')
        fig_sum.update_layout(showlegend=False)
        # Add labels for number of days
        for i, row in window_df.iterrows():
            fig_sum.add_annotation(x=row['Year'], y=row['Total_CO2_Sum']/2,
                                   text=f"({row['Days']}d)", showarrow=False,
                                   font=dict(color="white", size=10))
        st.plotly_chart(fig_sum, use_container_width=True)

    with c_bar2:
        st.subheader("🔥 Average Daily Intensity Comparison")
        fig_avg = px.bar(window_df, x='Year', y='Avg_CO2_Daily', 
                         color='Avg_CO2_Daily', color_continuous_scale='RdYlGn_r',
                         text_auto='.2f',
                         labels={'Avg_CO2_Daily': 'Avg Mt/day'})
        fig_avg.update_traces(textposition='outside')
        fig_avg.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_avg, use_container_width=True)

    st.divider()
    with st.expander("View Raw Data Details"):
        st.write(df.sort_values('Date', ascending=False).head(50))
