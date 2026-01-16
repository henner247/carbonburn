import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import os
from datetime import datetime, timedelta
import time
import urllib3

# --- 1. STREAMLIT CONFIG ---
st.set_page_config(page_title="EU27 Power Data Viewer", layout="wide")

# --- 2. KONFIGURATION ---
FILENAME = 'energy_charts_EU_hourly.csv'

# SSL-Warnungen unterdrücken
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Einstellungen für den Request (Standard Web)
VERIFY_SSL = True # Im öffentlichen Web sollte SSL aktiv sein (Sicherheit)
TIMEOUT = 30
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# --- 3. BACKEND: DATEN UPDATE ---

def fetch_and_update_eu_power():
    """Lädt fehlende Daten herunter und speichert sie in der CSV."""
    status_container = st.sidebar.empty()
    status_container.info("Prüfe auf neue Daten...")
    
    base_url = "https://api.energy-charts.info/public_power"
    country = "eu"
    current_year = datetime.now().year
    
    # 1. Bestandsaufnahme
    existing_df = None
    start_date_obj = datetime(2023, 1, 1)
    
    if os.path.exists(FILENAME):
        try:
            existing_df = pd.read_csv(FILENAME, parse_dates=['datetime'], index_col='datetime')
            if not existing_df.empty:
                # Wir starten beim letzten bekannten Zeitstempel
                start_date_obj = existing_df.index.max()
        except Exception:
            existing_df = None
    
    # 2. Download Loop
    new_dfs = []
    download_start_year = start_date_obj.year
    
    # Wir laden bis zum aktuellen Jahr inkl. heute (Filterung passiert später bei der Anzeige)
    years_to_download = list(range(download_start_year, current_year + 1))
    
    # Progress Bar in der Sidebar
    progress_bar = st.sidebar.progress(0)
    
    for i, year in enumerate(years_to_download):
        
        # Start-Datum festlegen
        if year == download_start_year:
            s_date = start_date_obj.strftime("%Y-%m-%d")
        else:
            s_date = f"{year}-01-01"
            
        # End-Datum festlegen
        e_date = f"{year}-12-31"
        if year == current_year:
            e_date = datetime.now().strftime("%Y-%m-%d")
            
        url = f"{base_url}?country={country}&start={s_date}&end={e_date}"
        
        try:
            # Einfacher Request ohne Proxy
            response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            timestamps = data.get('unix_seconds')
            if timestamps:
                # DataFrame erstellen
                df_temp = pd.DataFrame({'timestamp': timestamps})
                for source in data.get('production_types', []):
                    if len(source.get('data')) == len(timestamps):
                        df_temp[source.get('name')] = source.get('data')
                
                # Datetime konvertieren
                df_temp['datetime'] = pd.to_datetime(df_temp['timestamp'], unit='s')
                df_temp.set_index('datetime', inplace=True)
                df_temp.drop(columns=['timestamp'], inplace=True)
                
                # Resample auf Stunden (Mittelwert)
                df_hourly = df_temp.resample('h').mean()
                new_dfs.append(df_hourly)
                
            time.sleep(0.2) # Kurze Pause für API-Fairness
            
        except Exception as e:
            st.sidebar.error(f"Fehler bei Jahr {year}: {e}")
        
        # Fortschrittsbalken aktualisieren
        progress_bar.progress((i + 1) / len(years_to_download))

    progress_bar.empty() # Balken entfernen wenn fertig

    # 3. Speichern und Zusammenfügen
    if new_dfs:
        fresh_data = pd.concat(new_dfs)
        
        if existing_df is not None:
            full_df = pd.concat([existing_df, fresh_data])
            # Duplikate entfernen (die neuesten behalten -> Updates für den laufenden Tag)
            full_df = full_df[~full_df.index.duplicated(keep='last')]
        else:
            full_df = fresh_data
            
        full_df.sort_index(inplace=True)
        full_df.to_csv(FILENAME)
        
        status_container.success(f"Update erfolgreich! ({len(fresh_data)} Stunden geladen)")
        return True
    else:
        status_container.info("Daten sind aktuell.")
        return False

# --- 4. FRONTEND: DATEN LADEN & ANZEIGEN ---

@st.cache_data
def load_and_process_data():
    if not os.path.exists(FILENAME):
        return None
    
    # CSV laden
    df = pd.read_csv(FILENAME, parse_dates=['datetime'], index_col='datetime')
    
    # --- FILTER: HEUTE ABSCHNEIDEN ---
    # Wir nehmen "jetzt" und normalisieren auf 00:00 Uhr
    today_midnight = pd.Timestamp.now().normalize()
    
    # Behalte nur Daten, die ECHT KLEINER als heute 00:00 sind
    # (also alles bis gestern 23:59)
    df = df[df.index < today_midnight]
    
    if df.empty:
        return df # Falls noch gar keine alten Daten da sind

    # Resample auf Tageswerte (Summe)
    df_daily = df.resample('D').sum()
    
    # CO2 Proxy Berechnung
    cols = df_daily.columns
    lignite = df_daily['Fossil brown coal / lignite'] if 'Fossil brown coal / lignite' in cols else 0
    hard_coal = df_daily['Fossil hard coal'] if 'Fossil hard coal' in cols else 0
    gas = df_daily['Fossil gas'] if 'Fossil gas' in cols else 0
    
    df_daily['CO2_Proxy'] = (lignite * 1.15) + (hard_coal * 0.85) + (gas * 0.45)
    
    # Metadaten für Plotly
    df_daily['Year'] = df_daily.index.year
    df_daily['DayOfYear'] = df_daily.index.dayofyear
    df_daily['Date'] = df_daily.index.date
    
    return df_daily

# --- UI LOGIK ---

st.sidebar.title("EU27 Power Data")

# UPDATE BUTTON
if st.sidebar.button("🔄 Auf Updates prüfen"):
    updated = fetch_and_update_eu_power()
    if updated:
        st.cache_data.clear() # Cache leeren
        st.rerun()

st.sidebar.markdown("---")

# DROPDOWN
tech_mapping = {
    'CO2_Proxy': '🏭 CO2 Burn Schätzer (Fossil)',
    'Fossil brown coal / lignite': 'Braunkohle (Lignite)',
    'Fossil hard coal': 'Steinkohle (Hard Coal)',
    'Fossil gas': 'Erdgas',
    'Nuclear': 'Atomkraft',
    'Wind onshore': 'Wind an Land',
    'Wind offshore': 'Wind auf See',
    'Solar': 'Solar',
    'Load': 'Stromverbrauch (Load)'
}

selected_label = st.sidebar.selectbox(
    "Metrik wählen:", 
    options=list(tech_mapping.values()),
    index=0
)
selected_col = [k for k, v in tech_mapping.items() if v == selected_label][0]

# DATEN LADEN
df_daily = load_and_process_data()

# FALLBACK WENN KEINE DATEI DA IST
if df_daily is None:
    st.warning(f"Die Datei '{FILENAME}' wurde noch nicht gefunden.")
    st.info("Klicken Sie links auf **'🔄 Auf Updates prüfen'**, um den ersten Download zu starten.")
    st.stop()

# MAIN PAGE
latest_date = df_daily.index.max()
# Fallback, falls DataFrame leer ist nach Filterung
latest_str = latest_date.strftime('%d.%m.%Y') if pd.notnull(latest_date) else "N/A"

st.title("EU27 Power Data Viewer")
st.markdown(f"**Anzeige:** {selected_label} | **Daten verfügbar bis:** {latest_str} (exkl. Heute)")

if selected_col == 'CO2_Proxy':
    st.info("ℹ️ **CO2-Schätzer:** (Braunkohle × 1.15) + (Steinkohle × 0.85) + (Gas × 0.45). Werte in kt CO2.")

# CHART VORBEREITUNG
plot_df = df_daily[[selected_col, 'Year', 'DayOfYear', 'Date']].copy()
plot_df['Cumulative'] = plot_df.groupby('Year')[selected_col].cumsum()
plot_df['Rolling7Day'] = plot_df[selected_col].rolling(window=7).mean()

# CHART 1
st.subheader("1) Kumulierte Jahresproduktion (YTD)")
fig_cumsum = px.line(plot_df, x='DayOfYear', y='Cumulative', color='Year',
                     title=f"YTD Summe: {selected_label}",
                     labels={'DayOfYear': 'Tag des Jahres', 'Cumulative': 'Summe (GWh / kt)', 'Year': 'Jahr'},
                     hover_data=['Date'])
fig_cumsum.update_traces(line=dict(width=2.5))
fig_cumsum.update_layout(hovermode="x unified")
st.plotly_chart(fig_cumsum, use_container_width=True)

# CHART 2
st.subheader("2) 7-Tage-Durchschnitt")
fig_rolling = px.line(plot_df, x='DayOfYear', y='Rolling7Day', color='Year',
                      title=f"Trend: {selected_label}",
                      labels={'DayOfYear': 'Tag des Jahres', 'Rolling7Day': 'Tageswert Ø (GWh / kt)', 'Year': 'Jahr'},
                      hover_data=['Date'])
fig_rolling.update_traces(line=dict(width=2.5))
fig_rolling.update_layout(hovermode="x unified")
st.plotly_chart(fig_rolling, use_container_width=True)

# TABELLE
with st.expander("Rohdaten ansehen"):
    st.dataframe(df_daily[[selected_col]].sort_index(ascending=False).round(1))