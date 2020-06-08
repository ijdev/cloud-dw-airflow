from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator, LoadDimensionOperator, LoadFactOperator, DataQualityOperator
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# PROVIDE YOUR  (( CONN_ID ))  TO REDSHIFT & AWS_CREDENTIALS
REDSHIFT_CONN_ID = 'redshift'
AWS_CREDENTIALS = 'aws_credentials'
# PROVIDE YOUR S3 BUCKET (IT SHOULD BE LOCATED IN THE SAME LOCATION OF YOUR CLUSTER)
S3_BUCKET = 'cap-dend'
# THE NAME OF DATA I HAVE PROVIDED FOR YOU
S3_KEY_I94 = 'i94.csv'
# S3_KEY_DEMO = 'demographic.csv'
S3_KEY_DEMO = 'demographic.json'
JSONPATH = 's3://cap-dend/jsonpaths.json'
S3_KEY_C = 'country.csv'
S3_KEY_P = 'port.csv'
S3_KEY_V = 'visa.csv'

default_args = {
    'owner': 'ibrahem',
    'depends_on_past': False,
    'email_on_retry': False
}

dag = DAG('capstone',
          default_args=default_args,
          start_date=datetime(2020, 6, 1),
          catchup=False,
          description='Load and transform data in Redshift with Airflow - Capstone project',
          max_active_runs=1
          #schedule_interval='0 * * * *',
          )

start_operator = DummyOperator(task_id='start',  dag=dag)

# DROP ALL TABLES
drop_tables_task = PostgresOperator(
    task_id="drop_tables",
    dag=dag,
    sql='drop_tables.sql',
    postgres_conn_id=REDSHIFT_CONN_ID
)

#CREATE ALL TABLES
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id=REDSHIFT_CONN_ID
)

# LOAD THE STAGING AND LOOKUP TABLE

stage_i94_to_redshift = StageToRedshiftOperator(
    task_id='i94staging',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_I94,
    table='i94staging',
    deli=','
)

stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='demo_staging',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_DEMO,
    table='demo_staging',
    # deli=';'
    json=JSONPATH
)

load_country_lookup = StageToRedshiftOperator(
    task_id='country_lookup',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_C,
    table='country_lookup',
    deli=','
)

load_port_lookup = StageToRedshiftOperator(
    task_id='port_lookup',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_P,
    table='port_lookup',
    deli=','
)

load_visa_lookup = StageToRedshiftOperator(
    task_id='visa_lookup',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    s3_bucket=S3_BUCKET,
    s3_key=S3_KEY_V,
    table='visa_lookup',
    deli=','
)

# LOAD THE FACT AND DIMENSIONS TABLES

load_i94fact_table = LoadFactOperator(
    task_id='load_fact_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql_stmt=SqlQueries.i94fact_table_insert
)

load_state_dim = LoadDimensionOperator(
    task_id='Load_state_dim',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    truncate_table='state',
    sql_stmt=SqlQueries.state_table_insert
)
load_city_dim = LoadDimensionOperator(
    task_id='Load_city_dim',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    truncate_table='city',
    sql_stmt=SqlQueries.city_table_insert
)
load_country_dim = LoadDimensionOperator(
    task_id='Load_country_dim',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    truncate_table='country',
    sql_stmt=SqlQueries.country_table_insert
)


load_port_dim = LoadDimensionOperator(
    task_id='Load_port_dim',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    truncate_table='port',
    sql_stmt=SqlQueries.port_table_insert
)


load_visa_dim = LoadDimensionOperator(
    task_id='Load_visa_dim',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    truncate_table='visa',
    sql_stmt=SqlQueries.visa_table_insert
)


# SIMPLE DATA QUALITY CHECK
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id=REDSHIFT_CONN_ID,
    data_quality_checks=[
        {'sql': 'SELECT COUNT(*) FROM i94fact WHERE ccid IS NULL', 'exp': 0},
        {'sql': 'SELECT COUNT(*) FROM visa WHERE visa_code IS NULL', 'exp': 0},
        {'sql': 'SELECT COUNT(*) FROM country WHERE code IS NULL', 'exp': 0},
        {'sql': 'SELECT COUNT(*) FROM port WHERE port_code IS NULL', 'exp': 0},
        {'sql': 'SELECT COUNT(*) FROM state WHERE state_code IS NULL', 'exp': 0},
        {'sql': 'SELECT COUNT(*) FROM city WHERE city IS NULL', 'exp': 0}
    ]
)

end_operator = DummyOperator(task_id='end',  dag=dag)

# To orginize the strcture of the dag lines.
p_op = DummyOperator(task_id='i94_port',  dag=dag)
s_op = DummyOperator(task_id='i94_demo',  dag=dag)
c_op = DummyOperator(task_id='i94_country',  dag=dag)
v_op = DummyOperator(task_id='i94_visa',  dag=dag)

start_operator >> drop_tables_task
drop_tables_task >> create_tables_task

create_tables_task >> stage_demographic_to_redshift
create_tables_task >> stage_i94_to_redshift

create_tables_task >> load_country_lookup
create_tables_task >> load_port_lookup
create_tables_task >> load_visa_lookup

stage_i94_to_redshift >> s_op
stage_demographic_to_redshift >> s_op
s_op >> load_state_dim

load_state_dim >> load_city_dim

load_country_lookup >> c_op
stage_i94_to_redshift >> c_op
c_op >> load_country_dim

load_visa_lookup >> v_op
stage_i94_to_redshift >> v_op
v_op >> load_visa_dim

load_port_lookup >> p_op
stage_i94_to_redshift >> p_op
p_op >> load_port_dim


load_state_dim >> load_i94fact_table
load_country_dim >> load_i94fact_table
load_visa_dim >> load_i94fact_table
load_port_dim >> load_i94fact_table

load_i94fact_table >> run_quality_checks
load_city_dim >> run_quality_checks
run_quality_checks >> end_operator
