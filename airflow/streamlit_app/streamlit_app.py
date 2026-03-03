"""
Gold Price Analysis & Forecasting Dashboard
Complete interactive dashboard with historical data and AI predictions
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os
import numpy as np

# Add path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../include/utils'))
from db_connector import DatabaseConnector

# Page configuration
st.set_page_config(
    page_title="Gold Price Forecasting Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #FFD700, #FFA500);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db_connection():
    return DatabaseConnector()


@st.cache_data(ttl=300)
def load_historical_data(_db):
    return _db.get_all_gold_prices()


@st.cache_data(ttl=300)
def load_forecasts(_db, model_name=None):
    return _db.get_forecasts(model_name)


@st.cache_data(ttl=300)
def load_model_performance(_db):
    return _db.get_model_performance()


@st.cache_data(ttl=300)
def load_summary_stats(_db):
    return _db.get_summary_stats()


def calculate_kpis(df_filtered, df_full=None):
    """
    Calculate KPIs.
    - df_filtered : date-range filtered data (for current price, ATH, ATL, volatility)
    - df_full     : full unfiltered dataset  (for total return — always from day 1)
    """
    if df_filtered.empty or len(df_filtered) < 2:
        return {}

    latest   = df_filtered.iloc[-1]
    previous = df_filtered.iloc[-2]

    # ✅ Total return always calculated from the very first record in the full dataset
    first = df_full.iloc[0] if df_full is not None and not df_full.empty else df_filtered.iloc[0]

    daily_change     = latest['close'] - previous['close']
    daily_change_pct = (daily_change / previous['close']) * 100
    total_return     = ((latest['close'] - first['close']) / first['close']) * 100

    if len(df_filtered) >= 30:
        volatility = df_filtered['close'].pct_change().tail(30).std() * 100
    else:
        volatility = 0

    return {
        'current_price':    latest['close'],
        'daily_change':     daily_change,
        'daily_change_pct': daily_change_pct,
        'all_time_high':    df_filtered['high'].max(),
        'all_time_low':     df_filtered['low'].min(),
        'total_return':     total_return,
        'volatility_30d':   volatility,
        'avg_volume':       df_filtered['volume'].mean(),
        'latest_date':      latest['date']
    }


def plot_historical_with_forecast(historical_df, forecast_df):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=historical_df['date'],
        y=historical_df['close'],
        mode='lines',
        name='Historical Price',
        line=dict(color='#1f77b4', width=2),
        hovertemplate='Date: %{x}<br>Price: $%{y:,.2f}<extra></extra>'
    ))

    if not forecast_df.empty:
        fig.add_trace(go.Scatter(
            x=forecast_df['forecast_date'],
            y=forecast_df['predicted_price'],
            mode='lines',
            name='AI Forecast',
            line=dict(color='#ff7f0e', width=3, dash='dash'),
            hovertemplate='Date: %{x}<br>Forecast: $%{y:,.2f}<extra></extra>'
        ))

        fig.add_trace(go.Scatter(
            x=forecast_df['forecast_date'],
            y=forecast_df['upper_bound'],
            mode='lines',
            name='Upper Bound',
            line=dict(width=0),
            showlegend=False,
            hoverinfo='skip'
        ))

        fig.add_trace(go.Scatter(
            x=forecast_df['forecast_date'],
            y=forecast_df['lower_bound'],
            mode='lines',
            name='95% Confidence Interval',
            fill='tonexty',
            fillcolor='rgba(255,127,14,0.2)',
            line=dict(width=0),
            hovertemplate='Lower: $%{y:,.2f}<extra></extra>'
        ))

    fig.update_layout(
        title={'text': 'Gold Price: Historical Data + AI Forecast', 'font': {'size': 20, 'color': '#2c3e50'}},
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
    )

    return fig


def plot_price_comparison(df):
    fig = go.Figure()
    colors = {'open': '#3498db', 'high': '#2ecc71', 'low': '#e74c3c', 'close': '#f39c12'}

    for col in ['open', 'high', 'low', 'close']:
        fig.add_trace(go.Scatter(
            x=df['date'], y=df[col],
            mode='lines', name=col.capitalize(),
            line=dict(width=2, color=colors[col]),
            hovertemplate='%{y:$,.2f}<extra></extra>'
        ))

    fig.update_layout(
        title={'text': 'Price Comparison (Open, High, Low, Close)', 'font': {'size': 18}},
        xaxis_title='Date', yaxis_title='Price (USD)',
        hovermode='x unified', template='plotly_white', height=500
    )
    return fig


def plot_candlestick(df):
    fig = go.Figure(data=[go.Candlestick(
        x=df['date'],
        open=df['open'], high=df['high'],
        low=df['low'],   close=df['close'],
        name='Gold Price',
        increasing_line_color='#2ecc71',
        decreasing_line_color='#e74c3c'
    )])

    fig.update_layout(
        title={'text': 'Gold Price Candlestick Chart', 'font': {'size': 18}},
        xaxis_title='Date', yaxis_title='Price (USD)',
        template='plotly_white', height=500,
        xaxis_rangeslider_visible=False
    )
    return fig


def plot_volume_analysis(df):
    colors = ['#e74c3c' if df.iloc[i]['close'] < df.iloc[i]['open'] else '#2ecc71'
              for i in range(len(df))]

    fig = go.Figure(data=[go.Bar(
        x=df['date'], y=df['volume'],
        marker_color=colors, name='Volume',
        hovertemplate='Volume: %{y:,.0f}<extra></extra>'
    )])

    fig.update_layout(
        title={'text': 'Trading Volume Analysis', 'font': {'size': 18}},
        xaxis_title='Date', yaxis_title='Volume',
        template='plotly_white', height=400
    )
    return fig


def plot_forecast_vs_actual(historical_df, forecast_df, test_start_date):
    test_actual = historical_df[historical_df['date'] >= pd.to_datetime(test_start_date)].copy()

    if test_actual.empty or forecast_df.empty:
        return None

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=test_actual['date'], y=test_actual['close'],
        mode='lines+markers', name='Actual Price',
        line=dict(color='#2ecc71', width=2), marker=dict(size=6),
        hovertemplate='Actual: $%{y:,.2f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=forecast_df['forecast_date'], y=forecast_df['predicted_price'],
        mode='lines+markers', name='Predicted Price',
        line=dict(color='#e74c3c', width=2, dash='dash'), marker=dict(size=6),
        hovertemplate='Predicted: $%{y:,.2f}<extra></extra>'
    ))

    fig.update_layout(
        title={'text': 'Forecast vs Actual Prices (Test Set)', 'font': {'size': 18}},
        xaxis_title='Date', yaxis_title='Price (USD)',
        hovermode='x unified', template='plotly_white',
        height=500, legend=dict(x=0.01, y=0.99)
    )
    return fig


def plot_moving_averages(df):
    df = df.copy()
    df['MA_7']  = df['close'].rolling(window=7).mean()
    df['MA_30'] = df['close'].rolling(window=30).mean()
    df['MA_90'] = df['close'].rolling(window=90).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['date'], y=df['close'],  mode='lines', name='Close Price',
                             line=dict(color='lightgray', width=1), opacity=0.5))
    fig.add_trace(go.Scatter(x=df['date'], y=df['MA_7'],   mode='lines', name='7-Day MA',
                             line=dict(color='#3498db', width=2)))
    fig.add_trace(go.Scatter(x=df['date'], y=df['MA_30'],  mode='lines', name='30-Day MA',
                             line=dict(color='#f39c12', width=2)))
    fig.add_trace(go.Scatter(x=df['date'], y=df['MA_90'],  mode='lines', name='90-Day MA',
                             line=dict(color='#e74c3c', width=2)))

    fig.update_layout(
        title={'text': 'Price Trends with Moving Averages', 'font': {'size': 18}},
        xaxis_title='Date', yaxis_title='Price (USD)',
        hovermode='x unified', template='plotly_white', height=500
    )
    return fig


def display_model_metrics(metrics_df, df_forecasts=None):
    if metrics_df.empty:
        st.warning("⚠️ No model performance data available")
        return

    # ✅ Parse created_at as datetime then sort — picks truly latest row
    metrics_df = metrics_df.copy()
    metrics_df['created_at'] = pd.to_datetime(metrics_df['created_at'])
    latest_metrics = metrics_df.sort_values('created_at', ascending=False).iloc[0]

    # ✅ Clean retrieval of train/test size
    train_size = int(latest_metrics.get('train_size', 0) or 0)
    test_size  = int(latest_metrics.get('test_size',  0) or 0)

    st.subheader("🤖 Model Performance Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="📊 RMSE",  value=f"${latest_metrics['rmse']:.2f}",
                  help="Root Mean Square Error - Lower is better")
    with col2:
        st.metric(label="📉 MAE",   value=f"${latest_metrics['mae']:.2f}",
                  help="Mean Absolute Error - Average prediction error")
    with col3:
        st.metric(label="📈 MAPE",  value=f"{latest_metrics['mape']:.2f}%",
                  help="Mean Absolute Percentage Error")
    with col4:
        st.metric(label="🎯 Model", value=latest_metrics['model_name'],
                  help="Forecasting model used")

    with st.expander("📋 Detailed Model Information"):
        col1, col2 = st.columns(2)

        with col1:
            st.write("**Training Information:**")
            st.write(f"- Training Size: {train_size:,} records")
            st.write(f"- Test Size: {test_size:,} records")
            st.write(f"- Training Date: {latest_metrics.get('training_date', 'N/A')}")
            st.write(f"- Model: {latest_metrics['model_name']}")

        with col2:
            st.write("**Test Period:**")
            if df_forecasts is not None and not df_forecasts.empty:
                st.write(f"- Start Date: {df_forecasts['forecast_date'].min().strftime('%Y-%m-%d')}")
                st.write(f"- End Date: {df_forecasts['forecast_date'].max().strftime('%Y-%m-%d')}")
            else:
                st.write("- Start Date: N/A")
                st.write("- End Date: N/A")

    # Model comparison table
    if len(metrics_df) > 1:
        st.subheader("📊 Model Comparison")
        comparison_df = metrics_df[['model_name', 'rmse', 'mae', 'mape', 'training_date']].copy()
        comparison_df.columns = ['Model', 'RMSE', 'MAE', 'MAPE (%)', 'Training Date']
        st.dataframe(comparison_df, use_container_width=True, hide_index=True)


def main():
    st.markdown('<h1 class="main-header">💰 Gold Price Forecasting Dashboard</h1>',
                unsafe_allow_html=True)
    st.markdown(
        '<p style="text-align: center; color: gray; font-size: 1.1rem;">'
        'Real-time Data Analysis & Predictions</p>',
        unsafe_allow_html=True
    )

    db = get_db_connection()

    # ── Sidebar ───────────────────────────────────────────────────────
    st.sidebar.title("⚙️ Dashboard Controls")

    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # ── Load data ─────────────────────────────────────────────────────
    with st.spinner("📊 Loading data from database..."):
        df_historical = load_historical_data(db)
        df_forecasts  = load_forecasts(db)
        df_metrics    = load_model_performance(db)
        summary_stats = load_summary_stats(db)

    if df_historical.empty:
        st.error("❌ No data available in the database. Please run the Airflow pipeline first.")
        st.info("💡 Run the pipeline to scrape and store gold price data.")
        return

    # ── Date filter ───────────────────────────────────────────────────
    st.sidebar.subheader("📅 Date Range Filter")

    min_date = df_historical['date'].min().date()
    max_date = df_historical['date'].max().date()

    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start Date",
                                   value=max_date - timedelta(days=180),
                                   min_value=min_date, max_value=max_date)
    with col2:
        end_date = st.date_input("End Date",
                                 value=max_date,
                                 min_value=min_date, max_value=max_date)

    df_filtered = df_historical[
        (df_historical['date'].dt.date >= start_date) &
        (df_historical['date'].dt.date <= end_date)
    ].copy()

    st.sidebar.info(f"📊 Showing **{len(df_filtered):,}** records")

    # ── Chart toggles ─────────────────────────────────────────────────
    st.sidebar.subheader("📈 Chart Options")
    show_forecast    = st.sidebar.checkbox("Show AI Forecast",  value=True)
    show_candlestick = st.sidebar.checkbox("Candlestick Chart", value=False)
    show_volume      = st.sidebar.checkbox("Volume Analysis",   value=False)
    show_ma          = st.sidebar.checkbox("Moving Averages",   value=False)
    show_comparison  = st.sidebar.checkbox("OHLC Comparison",   value=True)

    # ✅ Pass full df_historical so total return is always from day 1
    kpis = calculate_kpis(df_filtered, df_historical)

    # ── KPI Banner ────────────────────────────────────────────────────
    st.subheader("📊 Key Performance Indicators")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        delta_color = "normal" if kpis.get('daily_change', 0) >= 0 else "inverse"
        st.metric("Current Price",
                  f"${kpis.get('current_price', 0):,.2f}",
                  delta=f"{kpis.get('daily_change_pct', 0):.2f}%",
                  delta_color=delta_color)
    with col2:
        st.metric("All-Time High",     f"${kpis.get('all_time_high', 0):,.2f}")
    with col3:
        st.metric("All-Time Low",      f"${kpis.get('all_time_low', 0):,.2f}")
    with col4:
        st.metric("Total Return",      f"{kpis.get('total_return', 0):.2f}%")
    with col5:
        st.metric("30-Day Volatility", f"{kpis.get('volatility_30d', 0):.2f}%")

    col6, col7, col8 = st.columns(3)
    with col6:
        st.metric("Average Volume",  f"{kpis.get('avg_volume', 0):,.0f}")
    with col7:
        # ✅ Use summary_stats from DB — always reflects full record count
        st.metric("Total Records",   f"{summary_stats.get('total_records', 0):,}")
    with col8:
        st.metric("Data Range",      f"{(max_date - min_date).days} days")

    st.markdown("---")

    # ── Main Chart ────────────────────────────────────────────────────
    st.subheader("📈 Historical Data & AI Forecast")
    forecast_data = df_forecasts if (show_forecast and not df_forecasts.empty) else pd.DataFrame()
    st.plotly_chart(plot_historical_with_forecast(df_filtered, forecast_data),
                    use_container_width=True)

    # ── Model Metrics ─────────────────────────────────────────────────
    if not df_metrics.empty:
        st.markdown("---")
        display_model_metrics(df_metrics, df_forecasts)

    # ── Forecast vs Actual ────────────────────────────────────────────
    if not df_metrics.empty and not df_forecasts.empty:
        st.markdown("---")
        st.subheader("🎯 Forecast Accuracy Analysis")
        test_start   = df_forecasts['forecast_date'].min()
        fig_accuracy = plot_forecast_vs_actual(df_historical, df_forecasts, test_start)
        if fig_accuracy:
            st.plotly_chart(fig_accuracy, use_container_width=True)

    # ── Optional Charts ───────────────────────────────────────────────
    st.markdown("---")

    if show_comparison:
        st.subheader("📊 Price Comparison (OHLC)")
        st.plotly_chart(plot_price_comparison(df_filtered), use_container_width=True)

    col_left, col_right = st.columns(2)

    with col_left:
        if show_candlestick:
            st.subheader("🕯️ Candlestick Chart")
            st.plotly_chart(plot_candlestick(df_filtered.tail(90)), use_container_width=True)
        if show_volume:
            st.subheader("📊 Trading Volume")
            st.plotly_chart(plot_volume_analysis(df_filtered), use_container_width=True)

    with col_right:
        if show_ma:
            st.subheader("📈 Moving Averages")
            st.plotly_chart(plot_moving_averages(df_filtered), use_container_width=True)

    # ── Raw Data Table ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📋 Raw Data Table")

    if st.checkbox("Show data table"):
        col1, col2 = st.columns(2)
        with col1:
            rows_to_show = st.selectbox("Rows to display:", [10, 25, 50, 100], index=1)
        with col2:
            sort_order = st.radio("Sort order:", ["Newest First", "Oldest First"], horizontal=True)

        display_df = df_filtered[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        display_df = display_df.sort_values('date', ascending=(sort_order == "Oldest First"))
        display_df = display_df.head(rows_to_show)
        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        display_df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.download_button(
            label="⬇️ Download Complete Dataset as CSV",
            data=df_filtered.to_csv(index=False),
            file_name=f"gold_prices_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )

    # ── Forecast Table ────────────────────────────────────────────────
    if not df_forecasts.empty:
        st.markdown("---")
        st.subheader("🔮 Forecast Data")

        if st.checkbox("Show forecast table"):
            forecast_display = df_forecasts[
                ['forecast_date', 'predicted_price', 'lower_bound', 'upper_bound', 'model_name']
            ].copy()
            forecast_display['forecast_date'] = forecast_display['forecast_date'].dt.strftime('%Y-%m-%d')
            forecast_display.columns = ['Date', 'Predicted Price', 'Lower Bound', 'Upper Bound', 'Model']

            st.dataframe(forecast_display, use_container_width=True, hide_index=True)

            st.download_button(
                label="⬇️ Download Forecast Data as CSV",
                data=df_forecasts.to_csv(index=False),
                file_name=f"gold_forecast_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )

    # ── Footer ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: gray; padding: 2rem;'>
            <h4>Gold Price Analysis & Forecasting Dashboard</h4>
            <p>📊 Data Source: yfinance (Gold Futures GC=F) | 🗄️ Database: PostgreSQL | 🤖 Model: ARIMA/SARIMA</p>
            <p>🔄 Pipeline: Apache Airflow | 📈 Visualization: Plotly & Streamlit</p>
            <p style='margin-top: 1rem;'>Last Updated: {}</p>
        </div>
    """.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), unsafe_allow_html=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ An error occurred: {str(e)}")
        st.info("💡 Please check your database connection and ensure the pipeline has run successfully.")
        import traceback
        with st.expander("🔍 Error Details"):
            st.code(traceback.format_exc())