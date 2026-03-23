# oil_prices_app.py
# Requirements: streamlit, requests, pandas, plotly

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
EIA_API_KEY  = st.secrets["EIA_API_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ── Supabase REST helpers ─────────────────────────────────────────────────────
def get_existing_dates() -> set:
    url = f"{SUPABASE_URL}/rest/v1/oil_prices?select=date"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return {row["date"] for row in resp.json()}

def upsert_rows(rows: list):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/oil_prices"
    headers = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    resp = requests.post(url, json=rows, headers=headers, timeout=30)
    resp.raise_for_status()

def load_all_data() -> pd.DataFrame:
    all_rows = []
    offset = 0
    page_size = 1000
    while True:
        url = f"{SUPABASE_URL}/rest/v1/oil_prices?select=*&order=date.asc&limit={page_size}&offset={offset}"
        headers = {**HEADERS, "Prefer": "count=exact"}
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        rows = resp.json()
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["wti", "brent", "wcs"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def get_latest_date_in_db() -> date | None:
    url = f"{SUPABASE_URL}/rest/v1/oil_prices?select=date&order=date.desc&limit=1"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        rows = resp.json()
        if rows:
            return date.fromisoformat(rows[0]["date"][:10])
    except Exception:
        pass
    return None

# ── EIA fetch (paginated) ─────────────────────────────────────────────────────
def fetch_eia_prices(series_id: str, start: str, end: str) -> dict:
    result = {}
    offset = 0
    page_size = 5000
    while True:
        url = (
            f"https://api.eia.gov/v2/petroleum/pri/spt/data/"
            f"?api_key={EIA_API_KEY}"
            f"&frequency=daily&data[0]=value"
            f"&facets[series][]={series_id}"
            f"&start={start}&end={end}"
            f"&sort[0][column]=period&sort[0][direction]=asc"
            f"&length={page_size}&offset={offset}"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        response = resp.json().get("response", {})
        data = response.get("data", [])
        for row in data:
            if row["value"] is not None:
                result[row["period"]] = row["value"]
        total = int(response.get("total", 0))
        offset += page_size
        if offset >= total or not data:
            break
    return result

# ── Alberta WCS fetch (fixed endpoint) ───────────────────────────────────────
def fetch_wcs_prices() -> dict:
    url = "https://api.economicdata.alberta.ca/data?table=OilPrices"
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        result = {}
        for entry in resp.json():
            if entry.get("Type ", "").strip() == "WCS":
                raw_date = entry.get("Date", "")
                price = entry.get("Value")
                if raw_date and price is not None:
                    d = raw_date[:10]
                    result[d] = float(price)
        return result
    except Exception as e:
        st.warning(f"WCS fetch failed: {e}")
        return {}

# ── Frankfurter: fetch USD/CAD rate ──────────────────────────────────────────
def fetch_usdcad_rate() -> float:
    try:
        resp = requests.get("https://api.frankfurter.dev/v1/latest?base=USD&symbols=CAD", timeout=60)
        resp.raise_for_status()
        return float(resp.json()["rates"]["CAD"])
    except Exception:
        return 1.38

# ── Sync ──────────────────────────────────────────────────────────────────────
def sync_data(silent=False):
    existing = get_existing_dates()
    today = date.today().isoformat()
    start = "2015-01-01"
    if silent:
        wti_data   = fetch_eia_prices("RWTC",  start, today)
        brent_data = fetch_eia_prices("RBRTE", start, today)
        wcs_data   = fetch_wcs_prices()
    else:
        with st.spinner("Fetching WTI & Brent from EIA..."):
            wti_data   = fetch_eia_prices("RWTC",  start, today)
            brent_data = fetch_eia_prices("RBRTE", start, today)
        with st.spinner("Fetching WCS from Alberta Economic Dashboard..."):
            wcs_data = fetch_wcs_prices()

    all_dates = set(wti_data) | set(brent_data) | set(wcs_data)
    new_rows = []
    for d in sorted(all_dates):
        if d not in existing:
            new_rows.append({
                "date":  d,
                "wti":   wti_data.get(d),
                "brent": brent_data.get(d),
                "wcs":   wcs_data.get(d),
            })

    wcs_update_rows = []
    for d, v in wcs_data.items():
        if d in existing:
            wcs_update_rows.append({"date": d, "wcs": v})

    if wcs_update_rows and not silent:
        with st.spinner(f"Updating {len(wcs_update_rows)} WCS values..."):
            for i in range(0, len(wcs_update_rows), 500):
                upsert_rows(wcs_update_rows[i:i+500])
    elif wcs_update_rows:
        for i in range(0, len(wcs_update_rows), 500):
            upsert_rows(wcs_update_rows[i:i+500])

    if new_rows:
        if silent:
            for i in range(0, len(new_rows), 500):
                upsert_rows(new_rows[i:i+500])
        else:
            with st.spinner(f"Saving {len(new_rows)} rows to Supabase..."):
                for i in range(0, len(new_rows), 500):
                    upsert_rows(new_rows[i:i+500])
            st.success(f"✅ Added {len(new_rows)} new records.")
    elif not silent:
        st.info("✅ Already up to date.")

# ── Weekly auto-sync (runs silently on app load) ──────────────────────────────
def maybe_auto_sync():
    if st.session_state.get("auto_sync_done"):
        return
    st.session_state["auto_sync_done"] = True
    latest = get_latest_date_in_db()
    if latest is None or (date.today() - latest).days > 7:
        sync_data(silent=True)

# ── Chart ─────────────────────────────────────────────────────────────────────
def render_chart(df, start_date, end_date, benchmarks, fx_rate=1.0, currency='USD'):
    mask = (df["date"] >= pd.Timestamp(start_date)) & (df["date"] <= pd.Timestamp(end_date))
    filtered = df[mask]
    colors = {"wti": "#F4A261", "brent": "#2A9D8F", "wcs": "#E76F51"}
    labels = {"wti": "WTI", "brent": "Brent", "wcs": "WCS"}
    fig = go.Figure()
    sym = "CA$" if currency == "CAD" else "$"
    for b in benchmarks:
        col_data = (filtered[b] * fx_rate).dropna()
        fig.add_trace(go.Scatter(
            x=filtered.loc[col_data.index, "date"],
            y=col_data,
            name=labels[b],
            line=dict(color=colors[b], width=2),
            hovertemplate=f"<b>{labels[b]}</b><br>Date: %{{x|%b %d, %Y}}<br>Price: {sym}%{{y:.2f}}/bbl<extra></extra>"
        ))
    fig.update_layout(
        title=f"Oil Benchmark Prices ({currency}/bbl)",
        title_font=dict(color="#333333"),
        xaxis_title="Date",
        yaxis_title=f"Price ({currency}/bbl)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="#333333"),
        xaxis=dict(
            gridcolor="#DDDDDD",
            linecolor="#333333",
            tickfont=dict(color="#333333"),
            title_font=dict(color="#333333"),
        ),
        yaxis=dict(
            gridcolor="#DDDDDD",
            linecolor="#333333",
            tickfont=dict(color="#333333"),
            title_font=dict(color="#333333"),
        ),
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True, key="main_chart")

# ── Safe metric helper (CAD-aware) ────────────────────────────────────────────
def show_metric(col, label, df, field, fx_rate=1.0, currency="USD", date_fmt="%b %d, %Y"):
    subset = df.dropna(subset=[field])
    if subset.empty:
        col.metric(label, "N/A", "No data yet")
    else:
        latest = subset.iloc[-1]
        price = latest[field] * fx_rate
        sym = "CA$" if currency == "CAD" else "$"
        col.metric(label, f"{sym}{price:.2f}", f"as of {latest['date'].strftime(date_fmt)}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="Oil Price Dashboard", page_icon="🛢️", layout="wide")
    st.title("🛢️ Oil Price Dashboard")
    st.caption("WTI & Brent: daily via EIA API · WCS: daily via Alberta Economic Dashboard")

    maybe_auto_sync()

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Refresh Data", use_container_width=True):
            sync_data()
            st.rerun()

    currency = st.radio("Currency", ["USD", "CAD"], horizontal=True)
    fx_rate = 1.0
    if currency == "CAD":
        fx_rate = fetch_usdcad_rate()
        st.caption(f"💱 Using USD/CAD rate: {fx_rate:.4f} (via Frankfurter, updated daily)")

    df = load_all_data()

    if df.empty:
        st.warning("No data yet — click **Refresh Data** to load prices.")
        return

    st.subheader("Latest Prices")
    m1, m2, m3 = st.columns(3)
    show_metric(m1, "WTI",   df, "wti",   fx_rate=fx_rate, currency=currency)
    show_metric(m2, "Brent", df, "brent", fx_rate=fx_rate, currency=currency)
    show_metric(m3, "WCS",   df, "wcs",   fx_rate=fx_rate, currency=currency)

    st.divider()

    st.subheader("Historical Chart")
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    default_start = max(min_date, max_date - timedelta(days=365))

    benchmarks = st.multiselect("Benchmarks", ["wti", "brent", "wcs"], default=["wti", "brent", "wcs"])

    st.markdown("**Adjust Date Range**")
    slider_range = st.slider(
        label="Date Range",
        min_value=min_date,
        max_value=max_date,
        value=(default_start, max_date),
        format="YYYY-MM-DD",
        label_visibility="collapsed"
    )
    start_date, end_date = slider_range[0], slider_range[1]

    if benchmarks:
        render_chart(df, start_date, end_date, benchmarks, fx_rate, currency)
    else:
        st.info("Select at least one benchmark above.")

    if "wti" in benchmarks and "wcs" in benchmarks:
        wcs_available = df.dropna(subset=["wcs"])
        if not wcs_available.empty:
            st.subheader("WTI–WCS Differential")
            diff_df = wcs_available.copy()
            diff_df["month"] = diff_df["date"].dt.to_period("M")
            wti_monthly = df.copy()
            wti_monthly["month"] = wti_monthly["date"].dt.to_period("M")
            wti_avg = wti_monthly.groupby("month")["wti"].mean().reset_index()
            merged = diff_df[["month","wcs"]].drop_duplicates("month").merge(wti_avg, on="month")
            merged["differential"] = merged["wti"] - merged["wcs"]
            merged["date"] = merged["month"].dt.to_timestamp()
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=merged["date"], y=merged["differential"],
                name="WTI–WCS Differential", marker_color="#E76F51",
                hovertemplate="<b>Differential</b><br>%{x|%b %Y}<br>$%{y:.2f}/bbl<extra></extra>"
            ))
            fig2.update_layout(
                title="WTI–WCS Monthly Differential (USD/bbl)",
                title_font=dict(color="#333333"),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="#333333"),
                xaxis=dict(gridcolor="#DDDDDD", linecolor="#333333", tickfont=dict(color="#333333")),
                yaxis=dict(gridcolor="#DDDDDD", linecolor="#333333", tickfont=dict(color="#333333")),
                height=350,
            )
            st.plotly_chart(fig2, use_container_chart=True, key="diff_chart")

    with st.expander("📋 View / Download Raw Data"):
        display_df = df.copy()
        display_df["date"] = display_df["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_df, use_container_width=True)
        csv = display_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", csv, "oil_prices.csv", "text/csv")

if __name__ == "__main__":
    main()
