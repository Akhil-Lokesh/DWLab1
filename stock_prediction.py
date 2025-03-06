from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta

# Fetch Alpha Vantage API Key from Airflow Variables
ALPHA_VANTAGE_API_KEY = Variable.get("alpha_vantage_api_key")

# Function to return Snowflake connection
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Define DAG
with DAG(
    dag_id="Stock_Prediction",
    start_date=datetime(2025, 2, 21),
    catchup=False,
    schedule_interval="30 2 * * *",  # Runs daily at 2:30 AM
    tags=["ML", "Stock"],
) as dag:

    # Define Snowflake table names
    train_input_table = "dev.raw_data.stock_data"
    train_view = "dev.adhoc.stock_data_view"
    forecast_table = "dev.adhoc.stock_data_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.final_stock_data"

    cur = return_snowflake_conn()

    @task
    def train():
        """
        Train a Snowflake ML model for stock price forecasting.
        """
        create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
            DATE, CLOSE, STOCK_SYMBOL
            FROM {train_input_table};"""

        create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'STOCK_SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );"""

        try:
            cur.execute(create_view_sql)
            cur.execute(create_model_sql)
            cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        except Exception as e:
            print(e)
            raise

    @task
    def predict():
        """
        Generate stock price predictions and store in Snowflake.
        """
        make_prediction_sql = f"""BEGIN
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;"""

        create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
            SELECT STOCK_SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {train_input_table}
            UNION ALL
            SELECT replace(series, '"', '') as STOCK_SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {forecast_table};"""

        try:
            cur.execute(make_prediction_sql)
            cur.execute(create_final_table_sql)
        except Exception as e:
            print(e)
            raise

    # Task Dependencies
    train_task = train()
    predict_task = predict()

    train_task >> predict_task 