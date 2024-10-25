from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
from datetime import datetime, timedelta


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_tables(cur):
    
	cur.execute(f"""
        CREATE TABLE IF NOT EXISTS user_session_db.raw_data.user_session_channel (
            userId int not NULL,
    	    sessionId varchar(32) primary key,
    	    channel varchar(32) default 'direct'
        )""")
	cur.execute(f"""
	CREATE TABLE IF NOT EXISTS user_session_db.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
    	    ts timestamp
        )""")
	
@task
def populate_tables(cur):
	cur.execute(f"""
            CREATE OR REPLACE STAGE user_session_db.raw_data.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"')
        """)
	cur.execute(f"""
            COPY INTO user_session_db.raw_data.user_session_channel
	    FROM @user_session_db.raw_data.blob_stage/user_session_channel.csv
        """)
	cur.execute(f"""
            COPY INTO user_session_db.raw_data.session_timestamp
	    FROM @user_session_db.raw_data.blob_stage/session_timestamp.csv
        """)

default_args = {
   'owner': 'ariel',
   'email': ['ariel.hsieh@sjsu.edu'],
   'retries': 1,
   'retry_delay': timedelta(minutes=3),
}


with DAG(
    dag_id = 'CreateTables',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *',
    default_args=default_args
) as dag:
    
    cur = return_snowflake_conn()
	
    create_tables(cur)
    populate_tables(cur)