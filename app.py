import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import os
from datetime import datetime
import time
import urllib3

# --- 1. STREAMLIT CONFIG (Muss ganz oben stehen) ---
st.set_page_config(page_title="EU27 Power Data Viewer", layout="wide")

# --- 2. KONFIGURATION & KONSTANTEN ---
FILENAME = 'energy_charts_EU_hourly.csv'

# SSL-Warnungen unterdrücken
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# NETZWERK-EINSTELLUNGEN
# WICHTIG FÜR GITHUB: Proxies niemals hardcoden, wenn das Repo öffentlich ist!
# Nutzen Sie stattdessen st.secrets oder Umgebungsvariablen.
# Für den lokalen Test oder interne Nutzung ist es hier okay.

# Versuch, Proxies aus Streamlit Secrets zu laden, sonst Fallback auf Hardcode

if "proxies" in st.secrets:
    PROXIES = st.secrets["proxies"]
else:
    PROXIES = None # Oder leer lassen, auf Streamlit Cloud braucht man meist keinen Proxy

VERIFY_SSL = False 
TIMEOUT = 25
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# --- 3. FUNKTIONEN: DATEN ABFRAGEN (BACKEND) ---

def check_integrity(df):
    """Prüft Datenqualität und gibt Status als Text zurück."""
    report = []
    if df.empty:
        return ["⚠️ DataFrame ist leer."]

    full_idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq='h')
    missing_timestamps = full_idx.difference(df.index)
    
    if len(missing_timestamps) == 0:
        report.append(f"✅ Zeitreihe vollständig ({len(df)} Stunden).")
    else:
        report.append(f"⚠️ Lücken entdeckt: {len(missing_timestamps)} Stunden fehlen.")

    if 'Load' in df.columns:
        zeros_load = (df['Load'] == 0).sum()
        if zeros_load > 0:
            report.append(f"⚠️ Warnung: 'Load' ist in {zeros_load} Stunden 0.")
    
    return report

def fetch_and_update_eu_power():
    """Die Update-Logik (Download nur neuer Daten)."""
    status_container = st.sidebar.empty() # Platzhalter für Statusmeldungen
    status_container.info("Starte Update-Prüfung...")
    
    base_url = "https://api.energy-charts.info/public_power"
    country = "eu"
    current_year = datetime.now().year
    
    # Bestandsaufnahme
    existing_df = None
    start_date_obj = datetime(2023, 1, 1)
    
    if os.path.exists(FILENAME):
        try:
            existing_df = pd.read_csv(FILENAME, parse_dates=['datetime'], index_col='datetime')
            if not existing_df.empty:
                last_timestamp = existing_df.index.max()
                start_date_obj = last_timestamp
        except Exception:
            existing_df = None
    
    # Download Loop
    new_dfs = []
    download_start_year = start_date_obj.year
    progress_bar = st.sidebar.progress(0)
    
    years_to_download = list(range(download_start_year, current_year + 1))
    
    for i, year in enumerate(years_to_download):
        status_container.text(f"Prüfe Jahr {year}...")
        
        if year == download_start_year:
            s_date = start_date_obj.strftime("%Y-%m-%d")
        else:
            s_date = f"{year}-01-01"
            
        e_date = f"{year}-12-31"
        if year == current_year:
            e_date = datetime.now().strftime("%Y-%m-%d")
            
        url = f"{base_url}?country={country}&start={s_date}&end={e_date}"
        
        try:
            response = requests.get(
                url, headers=HEADERS, proxies=PROXIES, 
                verify=VERIFY_SSL, timeout=TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            timestamps = data.get('unix_seconds')
            if timestamps:
                df_temp = pd.DataFrame({'timestamp': timestamps})
                for source in data.get('production_types', []):
                    if len(source.get('data')) == len(timestamps):
                        df_temp[source.get('name')] = source.get('data')
                
                df_temp['datetime'] = pd.to_datetime(df_temp['timestamp'], unit='s')
                df_temp.set_index('datetime', inplace=True)
                df_temp.drop(columns=['timestamp'], inplace=True)
                
                # Resample auf Stunden
                df_hourly = df_temp.resample('h').mean()
                new_dfs.append(df_hourly)
                
            time.sleep(0.5)
            
        except Exception as e:
            st.sidebar.error(f"Fehler bei {year}: {e}")
        
        progress_bar.progress((i + 1) / len(years_to_download))

    # Speichern
    if new_dfs:
        fresh_data = pd.concat(new_dfs)
        if existing_df is not None:
            full_df = pd.concat([existing_df, fresh_data])
            full_df = full_df[~full_df.index.duplicated(keep='last')]
        else:
            full_df = fresh_data
            
        full_df.sort_index(inplace=True)
        full_df.to_csv(FILENAME)
        
        status_container.success(f"Update fertig! Stand: {full_df.index.max()}")
        return True # Signal dass neue Daten da sind
    else:
        status_container.info("Daten waren bereits aktuell.")
        return False

# --- 4. STREAMLIT VISUALISIERUNG (FRONTEND) ---

# Caching-Funktion zum Laden
@st.cache_data
def load_data():
    if not os.path.exists(FILENAME):
        return None
    
    df = pd.read_csv(FILENAME, parse_dates=['datetime'], index_col='datetime')
    
    # Integritätscheck (nur Ausgabe in Console/Logs)
    check_integrity(df) 
    
    # Resample Daily
    df_daily = df.resample('D').sum()
    
    # CO2 Proxy Berechnung
    cols = df_daily.columns
    lignite = df_daily['Fossil brown coal / lignite'] if 'Fossil brown coal / lignite' in cols else 0
    hard_coal = df_daily['Fossil hard coal'] if 'Fossil hard coal' in cols else 0
    gas = df_daily['Fossil gas'] if 'Fossil gas' in cols else 0
    
    df_daily['CO2_Proxy'] = (lignite * 1.15) + (hard_coal * 0.85) + (gas * 0.45)
    
    # Metadaten
    df_daily['Year'] = df_daily.index.year
    df_daily['DayOfYear'] = df_daily.index.dayofyear
    df_daily['Date'] = df_daily.index.date
    
    return df_daily


# --- SIDEBAR UI ---
st.sidebar.title("EU27 Power Data Viewer")

# Button zum Aktualisieren
if st.sidebar.button("🔄 Neue Daten abrufen"):
    data_changed = fetch_and_update_eu_power()
    if data_changed:
        st.cache_data.clear() # WICHTIG: Cache löschen damit neue Daten laden
        st.rerun() # App neu laden

st.sidebar.markdown("---")

# Dropdown Menü
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
    "Technologie / Metrik:", 
    options=list(tech_mapping.values()),
    index=0
)
selected_col = [k for k, v in tech_mapping.items() if v == selected_label][0]


# --- MAIN UI ---
df = load_data()

if df is None:
    st.warning(f"Die Datei '{FILENAME}' wurde noch nicht gefunden.")
    st.info("Bitte klicken Sie links in der Sidebar auf **'🔄 Neue Daten abrufen'**, um den ersten Download zu starten.")
    st.stop()

latest_date = df.index.max()

st.title("EU27 Power Data Viewer")
st.markdown(f"**Auswahl:** {selected_label} | **Datenstand:** {latest_date.strftime('%d.%m.%Y')}")

if selected_col == 'CO2_Proxy':
    st.info("ℹ️ **Berechnungsgrundlage:** (Braunkohle × 1,15) + (Steinkohle × 0,85) + (Gas × 0,45). Werte in kt CO2.")

# Plot Vorbereitung
plot_df = df[[selected_col, 'Year', 'DayOfYear', 'Date']].copy()
plot_df['Cumulative'] = plot_df.groupby('Year')[selected_col].cumsum()
plot_df['Rolling7Day'] = plot_df[selected_col].rolling(window=7).mean()

# Chart 1
st.subheader("1) Kumulierte Produktion (YTD)")
fig_cumsum = px.line(plot_df, x='DayOfYear', y='Cumulative', color='Year',
                     title=f"YTD Summe: {selected_label}",
                     labels={'DayOfYear': 'Tag des Jahres', 'Cumulative': 'Summe (GWh / kt)', 'Year': 'Jahr'},
                     hover_data=['Date'])
fig_cumsum.update_traces(line=dict(width=2.5))
fig_cumsum.update_layout(hovermode="x unified")
st.plotly_chart(fig_cumsum, use_container_width=True)

# Chart 2
st.subheader("2) 7-Tage-Durchschnitt")
fig_rolling = px.line(plot_df, x='DayOfYear', y='Rolling7Day', color='Year',
                      title=f"7-Tage-Trend: {selected_label}",
                      labels={'DayOfYear': 'Tag des Jahres', 'Rolling7Day': 'Tageswert Ø (GWh / kt)', 'Year': 'Jahr'},
                      hover_data=['Date'])
fig_rolling.update_traces(line=dict(width=2.5))
fig_rolling.update_layout(hovermode="x unified")
st.plotly_chart(fig_rolling, use_container_width=True)

# Tabelle
with st.expander("Rohdaten ansehen"):
    st.dataframe(df[[selected_col]].sort_index(ascending=False).round(1))