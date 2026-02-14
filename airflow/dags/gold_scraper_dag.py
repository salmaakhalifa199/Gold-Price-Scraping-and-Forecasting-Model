"""
Gold Price Scraper DAG
Schedules daily gold price scraping with error handling and monitoring
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import yfinance as yf
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Define the DAG
dag = DAG(
    'gold_price_scraper',
    default_args=default_args,
    description='Scrape gold prices daily and save to CSV',
    schedule='@daily',  # Run daily at 9 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'gold', 'finance'],
)

def scrape_gold_prices_task(**context):
    """
    Scrape gold prices using yfinance
    """
    logger.info("Starting gold price scraping...")
    
    try:
        # Set parameters
        days = 365
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        logger.info(f"Fetching {days} days of data from {start_date.date()} to {end_date.date()}")
        
        # Download gold futures data (GC=F)
        gold = yf.Ticker("GC=F")
        df = gold.history(start=start_date, end=end_date)
        
        if df.empty:
            raise ValueError("No data retrieved from yfinance")
        
        # Reset index to make Date a column
        df = df.reset_index()
        
        # Rename and select columns
        df = df.rename(columns={
            'Date': 'Date',
            'Open': 'Open',
            'High': 'High', 
            'Low': 'Low',
            'Close': 'Close',
            'Volume': 'Volume'
        })
        
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        # Format date
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        # Create data directory in project root
        data_dir ='/usr/local/airflow/include/data'
        os.makedirs(data_dir, exist_ok=True)
        
        # Save to CSV
        filename = f"gold_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(data_dir, filename)
        df.to_csv(filepath, index=False)
        
        logger.info(f"✅ Successfully scraped {len(df)} records")
        logger.info(f"✅ Data saved to: {filepath}")
        logger.info(f"Price Range - Low: ${df['Low'].min():,.2f}, High: ${df['High'].max():,.2f}, Current: ${df['Close'].iloc[-1]:,.2f}")
        
        # Push filepath to XCom for downstream tasks
        context['ti'].xcom_push(key='filepath', value=filepath)
        context['ti'].xcom_push(key='record_count', value=len(df))
        
        return filepath
        
    except Exception as e:
        logger.error(f"❌ Error during scraping: {str(e)}")
        raise

def validate_data_task(**context):
    """
    Validate the scraped data
    """
    logger.info("Starting data validation...")
    
    # Get filepath from previous task
    filepath = context['ti'].xcom_pull(key='filepath', task_ids='scrape_gold_prices')
    
    if not filepath or not os.path.exists(filepath):
        raise ValueError(f"File not found: {filepath}")
    
    # Read and validate CSV
    df = pd.read_csv(filepath)
    
    # Check if empty
    if len(df) == 0:
        raise ValueError("CSV file is empty")
    
    # Check required columns
    required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for null values in critical columns
    null_counts = df[['Open', 'High', 'Low', 'Close']].isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"Found null values: {null_counts.to_dict()}")
    
    # Data quality checks
    if (df['High'] < df['Low']).any():
        raise ValueError("Data quality issue: High price is less than Low price")
    
    if (df['Close'] < 0).any() or (df['Open'] < 0).any():
        raise ValueError("Data quality issue: Negative prices found")
    
    logger.info(f"✅ Validation passed: {len(df)} records validated")
    logger.info(f"Date range: {df['Date'].iloc[0]} to {df['Date'].iloc[-1]}")
    
    return f"Validation successful for {filepath}"

def generate_summary_task(**context):
    """
    Generate a summary report of the scraped data
    """
    logger.info("Generating summary report...")
    
    filepath = context['ti'].xcom_pull(key='filepath', task_ids='scrape_gold_prices')
    df = pd.read_csv(filepath)
    
    summary = {
        'total_records': len(df),
        'date_range': f"{df['Date'].iloc[0]} to {df['Date'].iloc[-1]}",
        'lowest_price': float(df['Low'].min()),
        'highest_price': float(df['High'].max()),
        'current_price': float(df['Close'].iloc[-1]),
        'avg_price': float(df['Close'].mean()),
        'price_change': float(df['Close'].iloc[-1] - df['Close'].iloc[0]),
        'price_change_pct': float(((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0]) * 100)
    }
    
    logger.info("=" * 50)
    logger.info("GOLD PRICE SUMMARY REPORT")
    logger.info("=" * 50)
    logger.info(f"Total Records: {summary['total_records']}")
    logger.info(f"Date Range: {summary['date_range']}")
    logger.info(f"Lowest Price: ${summary['lowest_price']:,.2f}")
    logger.info(f"Highest Price: ${summary['highest_price']:,.2f}")
    logger.info(f"Current Price: ${summary['current_price']:,.2f}")
    logger.info(f"Average Price: ${summary['avg_price']:,.2f}")
    logger.info(f"Price Change: ${summary['price_change']:,.2f} ({summary['price_change_pct']:.2f}%)")
    logger.info("=" * 50)
    
    return summary

def cleanup_old_files_task(**context):
    """
    Cleanup files older than 30 days
    """
    logger.info("Starting cleanup of old files...")
    
    data_dir = '/usr/local/airflow/include/data'
    
    if not os.path.exists(data_dir):
        logger.info("No data directory to clean")
        return "No data directory to clean"
    
    cutoff_date = datetime.now() - timedelta(days=30)
    deleted_count = 0
    
    for filename in os.listdir(data_dir):
        if not filename.startswith('gold_prices_'):
            continue
            
        filepath = os.path.join(data_dir, filename)
        if os.path.isfile(filepath):
            file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
            if file_time < cutoff_date:
                os.remove(filepath)
                deleted_count += 1
                logger.info(f"Deleted old file: {filename}")
    
    logger.info(f"✅ Cleaned up {deleted_count} old files")
    return f"Cleaned up {deleted_count} old files"

# Define tasks - REMOVED provide_context=True
scrape_task = PythonOperator(
    task_id='scrape_gold_prices',
    python_callable=scrape_gold_prices_task,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_task,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary_task,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files_task,
    dag=dag,
)

# Set task dependencies
scrape_task >> validate_task >> summary_task >> cleanup_task