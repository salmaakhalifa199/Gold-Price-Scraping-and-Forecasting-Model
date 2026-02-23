"""
Gold Price Scraper DAG
Schedules daily gold price scraping with error handling and monitoring
"""
from multiprocessing import context

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import yfinance as yf
import logging
from io import StringIO

# Import from the utils package and the scrape function
import sys
sys.path.insert(0, '/usr/local/airflow/include/utils')
from scrape_gold import scrape_gold_prices
from data_cleaner import clean_gold_data
from eda_analyzer import perform_eda
from postgres_handler import PostgresHandler
from forecasting_model import (
    prepare_time_series_data,
    train_arima_model,
    make_predictions,
    evaluate_model,
    forecast_future_prices,
    save_forecast_to_db
)


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
    description='Complete gold price pipeline with forecasting',
    schedule='@daily',  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'gold', 'finance', 'data-analysis', 'forecasting', 'ml'],
)

def scrape_task_wrapper(**context):
    """Scrape gold prices"""
    logger.info("Starting gold price scraping task...")
    
    try:
        output_dir = '/usr/local/airflow/include/data'
        filepath = scrape_gold_prices(days=365, output_dir=output_dir)
        df = pd.read_csv(filepath)
        
        context['ti'].xcom_push(key='filepath', value=filepath)
        context['ti'].xcom_push(key='dataframe', value=df.to_json(orient='split', date_format='iso'))
        
        logger.info(f"✅ Scraping completed: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def clean_data_task(**context):
    """Clean and preprocess data"""
    logger.info("Starting data cleaning task...")
    
    try:
        df_json = context['ti'].xcom_pull(key='dataframe', task_ids='scrape_gold_prices')
        df = pd.read_json(StringIO(df_json), orient='split')
        
        cleaned_df, cleaning_report = clean_gold_data(df)
        
        output_dir = '/usr/local/airflow/include/data'
        cleaned_filename = f"gold_prices_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        cleaned_filepath = os.path.join(output_dir, cleaned_filename)
        cleaned_df.to_csv(cleaned_filepath, index=False)
        
        context['ti'].xcom_push(key='cleaned_filepath', value=cleaned_filepath)
        context['ti'].xcom_push(key='cleaned_dataframe', value=cleaned_df.to_json(orient='split', date_format='iso'))
        context['ti'].xcom_push(key='cleaning_report', value=cleaning_report)
        
        logger.info(f"✅ Cleaned data saved: {cleaned_filepath}")
        return cleaned_filepath
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise


def insert_to_postgres_task(**context):
    """Insert cleaned data to PostgreSQL"""
    logger.info("Starting PostgreSQL insertion...")
    
    try:
        import psycopg2
        
        df_json = context['ti'].xcom_pull(key='cleaned_dataframe', task_ids='clean_data')
        df = pd.read_json(StringIO(df_json), orient='split')
        
        # Standardize column names
        df.columns = df.columns.str.lower()
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        DB_PARAMS = {
            'host': 'host.docker.internal',  # For Docker
            'port': 5432,
            'database': 'gold_prices_db',
            'user': 'postgres',
            'password': 'postgres'
        }
        
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        inserted = 0
        updated = 0
        
        for _, row in df.iterrows():
            cursor.execute("""
              INSERT INTO gold_prices (
                price_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
              )
             VALUES (%s, %s, %s, %s, %s, %s)
             ON CONFLICT (price_date) DO UPDATE SET
             open_price = EXCLUDED.open_price,
             high_price = EXCLUDED.high_price,
             low_price = EXCLUDED.low_price,
             close_price = EXCLUDED.close_price,
             volume = EXCLUDED.volume,
             updated_at = CURRENT_TIMESTAMP
             RETURNING (xmax = 0) AS inserted
""", (
    row['date'],
    float(row['open']),
    float(row['high']),
    float(row['low']),
    float(row['close']),
    int(row['volume'])
))
            
            result = cursor.fetchone()
            if result[0]:
                inserted += 1
            else:
                updated += 1
        
        conn.commit()
        
        logger.info(f"✅ Database updated: {inserted} inserted, {updated} updated")
        
        cursor.close()
        conn.close()
        
        context['ti'].xcom_push(key='db_inserted', value=inserted)
        context['ti'].xcom_push(key='db_updated', value=updated)
        
        return f"Inserted: {inserted}, Updated: {updated}"
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def eda_task(**context):
    """Perform exploratory data analysis"""
    logger.info("Starting EDA task...")
    
    try:
        from io import StringIO
        import pandas as pd

        # Pull JSON from XCom
        df_json = context['ti'].xcom_pull(key='cleaned_dataframe', task_ids='clean_data')
        
        # <-- Add these two lines for debugging -->
        logger.info(f"type(df_json): {type(df_json)}")
        logger.info(f"Preview of df_json[:500]: {df_json[:500]}")  # first 500 chars
        
        # Read JSON into DataFrame
        df = pd.read_json(StringIO(df_json), orient='split')
        
        output_dir = '/usr/local/airflow/include/data/eda'
        stats_report, plot_paths = perform_eda(df, output_dir)
        
        context['ti'].xcom_push(key='eda_stats', value=stats_report)
        context['ti'].xcom_push(key='eda_plots', value=plot_paths)
        
        logger.info(f"✅ EDA complete! Generated {len(plot_paths)} visualizations")
        return stats_report

    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise


def run_forecasting(**context):
    import pandas as pd
    from io import StringIO
    import logging

    logger = logging.getLogger("airflow.task")

    logger.info("Starting forecasting task...")

    # --- Pull cleaned data from XCom ---
    df_json = context['ti'].xcom_pull(key='cleaned_dataframe', task_ids='clean_data')
    df = pd.read_json(StringIO(df_json), orient='split')

    # --- Ensure 'Date' is datetime and set as index ---
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        # Add daily frequency (important for ARIMA)
        df.index = pd.DatetimeIndex(df.index, freq='D')
    else:
        logger.warning("'Date' column not found. Using first column as index.")
        df.set_index(df.columns[0], inplace=True)
        df.index = pd.DatetimeIndex(df.index, freq='D')  # fallback

    logger.info(f"DataFrame columns after setting index: {df.columns.tolist()}")

    # --- Choose column for forecasting ---
    if 'closing_price' in df.columns:
        column_to_use = 'closing_price'
    elif 'Close' in df.columns:
        column_to_use = 'Close'
    else:
        column_to_use = df.columns[0]
        logger.warning(f"'closing_price' column not found. Using '{column_to_use}' instead.")

    logger.info(f"Using column '{column_to_use}' for forecasting")

    # --- Prepare time series ---
    train_data, test_data, _ = prepare_time_series_data(df, column=column_to_use)

    # --- Train ARIMA model ---
    model_order = (5, 1, 0)  # can optimize later
    model = train_arima_model(train_data, order=model_order)

    # --- Make predictions on test set ---
    forecast = model.get_forecast(steps=len(test_data))
    predictions = forecast.predicted_mean.astype(float)  # ensure float
    conf_int = forecast.conf_int()
    lower = conf_int.iloc[:, 0].astype(float)
    upper = conf_int.iloc[:, 1].astype(float)

    # --- Evaluate model ---
    metrics = evaluate_model(test_data.values, predictions.values)
    logger.info(f"Forecast Metrics: {metrics}")

    # --- Future forecast (next 30 days) ---
    future_forecast = forecast_future_prices(model, last_data=test_data, periods=30)

    # --- Push results to XCom ---
    context['ti'].xcom_push(key='forecast_metrics', value=metrics)

    predictions_df = pd.DataFrame({
        'predicted_price': predictions,
        'lower_bound': lower,
        'upper_bound': upper
    }, index=test_data.index)

    context['ti'].xcom_push(key='forecast_df', value=predictions_df.to_json(orient='split'))
    context['ti'].xcom_push(key='future_forecast', value=future_forecast.to_json(orient='split'))

    logger.info("✅ Forecasting task complete")
    
def save_forecast_to_db_task(**context):
    """Save forecast results to PostgreSQL"""
    logger.info("Saving forecast to database...")
    
    try:
        import psycopg2
        
        # Get forecast data and metrics
        forecast_json = context['ti'].xcom_pull(key='forecast_data', task_ids='run_forecasting')
        model_metrics = context['ti'].xcom_pull(key='model_metrics', task_ids='run_forecasting')
        
        forecast_df = pd.read_json(StringIO(forecast_json), orient='split')
        forecast_df['date'] = pd.to_datetime(forecast_df['date']).dt.date
        
        DB_PARAMS = {
            'host': 'host.docker.internal',
            'port': 5432,
            'database': 'gold_prices_db',
            'user': 'postgres',
            'password': 'postgres'
        }
        
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Clear old forecasts for this model
        cursor.execute("DELETE FROM gold_forecasts WHERE model_name = 'ARIMA'")
        
        # Insert new forecasts
        for _, row in forecast_df.iterrows():
            cursor.execute("""
                INSERT INTO gold_forecasts (forecast_date, predicted_price, lower_bound, upper_bound, model_name)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['date'],
                float(row['predicted_price']),
                float(row['lower_bound']),
                float(row['upper_bound']),
                'ARIMA'
            ))
        
        # Save model performance
        cursor.execute("""
            INSERT INTO model_performance 
            (model_name, rmse, mae, mape, training_date, test_start_date, test_end_date, train_size, test_size)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            'ARIMA',
            float(model_metrics['rmse']),
            float(model_metrics['mae']),
            float(model_metrics['mape']),
            datetime.now().date(),
            model_metrics['test_start'],
            model_metrics['test_end'],
            model_metrics['train_size'],
            model_metrics['test_size']
        ))
        
        conn.commit()
        
        logger.info(f"✅ Saved {len(forecast_df)} forecasts and metrics to database")
        
        cursor.close()
        conn.close()
        
        return f"Saved {len(forecast_df)} forecasts"
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def validate_data_task(**context):
    """Validate cleaned data"""
    logger.info("Starting validation...")
    
    try:
        filepath = context['ti'].xcom_pull(key='cleaned_filepath', task_ids='clean_data')
        df = pd.read_csv(filepath)
        
        if len(df) == 0:
            raise ValueError("Empty dataset")
        
        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        logger.info(f"✅ Validation passed: {len(df)} records")
        return f"Validation successful"
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def generate_summary_task(**context):
    """Generate comprehensive summary"""
    logger.info("Generating final summary report...")
    
    try:
        cleaning_report = context['ti'].xcom_pull(key='cleaning_report', task_ids='clean_data')
        eda_stats = context['ti'].xcom_pull(key='eda_stats', task_ids='perform_eda')
        model_metrics = context['ti'].xcom_pull(key='model_metrics', task_ids='run_forecasting')
        db_inserted = context['ti'].xcom_pull(key='db_inserted', task_ids='insert_to_postgres')
        db_updated = context['ti'].xcom_pull(key='db_updated', task_ids='insert_to_postgres')
        
        logger.info("=" * 70)
        logger.info("COMPLETE PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"")
        logger.info(f"1. DATA CLEANING:")
        logger.info(f"   - Duplicates Removed: {cleaning_report['duplicates_removed']}")
        logger.info(f"   - Missing Values Handled: {cleaning_report['missing_values_handled']}")
        logger.info(f"   - Outliers Detected: {cleaning_report['outliers_detected']}")
        logger.info(f"")
        logger.info(f"2. DATABASE STORAGE:")
        logger.info(f"   - Records Inserted: {db_inserted}")
        logger.info(f"   - Records Updated: {db_updated}")
        logger.info(f"")
        logger.info(f"3. EDA RESULTS:")
        logger.info(f"   - Total Records: {eda_stats['summary']['count']}")
        logger.info(f"   - Mean Price: ${eda_stats['summary']['price_statistics']['mean']:,.2f}")
        logger.info(f"   - Total Return: {eda_stats['trends']['total_return_pct']:.2f}%")
        logger.info(f"")
        logger.info(f"4. FORECASTING MODEL (ARIMA):")
        logger.info(f"   - RMSE: ${model_metrics['rmse']:.2f}")
        logger.info(f"   - MAE: ${model_metrics['mae']:.2f}")
        logger.info(f"   - MAPE: {model_metrics['mape']:.2f}%")
        logger.info(f"   - Training Size: {model_metrics['train_size']} records")
        logger.info(f"   - Test Size: {model_metrics['test_size']} records")
        logger.info("=" * 70)
        
        return {
            'cleaning': cleaning_report,
            'database': {'inserted': db_inserted, 'updated': db_updated},
            'eda': eda_stats,
            'forecasting': model_metrics
        }
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def cleanup_old_files_task(**context):
    """Cleanup old files"""
    logger.info("Starting cleanup...")
    
    data_dir = '/usr/local/airflow/include/data'
    cutoff_date = datetime.now() - timedelta(days=30)
    deleted_count = 0
    
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(data_dir, filename)
            if os.path.isfile(filepath):
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_time < cutoff_date:
                    os.remove(filepath)
                    deleted_count += 1
    
    logger.info(f"✅ Cleaned up {deleted_count} old files")
    return f"Cleaned up {deleted_count} files"

# Define tasks - REMOVED provide_context=True
scrape_task = PythonOperator(
    task_id='scrape_gold_prices',
    python_callable=scrape_task_wrapper,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,
    dag=dag,
)

postgres_task = PythonOperator(
    task_id='insert_to_postgres',
    python_callable=insert_to_postgres_task,
    dag=dag)

eda_task_op = PythonOperator(
    task_id='perform_eda',
    python_callable=eda_task,
    dag=dag,
)

forecasting_task = PythonOperator(
    task_id='run_forecasting',
    python_callable=run_forecasting,  # now uses the wrapped function
    retries=3,                         # optional: number of retries
    retry_delay=timedelta(minutes=5),  # optional: retry delay
    dag=dag
)

save_forecast_task = PythonOperator(
    task_id='save_forecast_to_db',
    python_callable=save_forecast_to_db_task,
    dag=dag)

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

# Task dependencies - Complete pipeline
scrape_task >> clean_task >> postgres_task
clean_task >> [eda_task_op, validate_task, forecasting_task]
forecasting_task >> save_forecast_task
[postgres_task, eda_task_op, validate_task, save_forecast_task] >> summary_task >> cleanup_task