from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator;
from airflow.operators.dummy_operator import DummyOperator;

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 11, 5),
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'provide_context': True
}

def stageUsersData():
    return SnowflakeOperator(
            task_id='stage-users-data',
            snowflake_conn_id='snowflake_connection',
            sql="put file:///opt/airflow/dags/files/sd254_users.csv @users_stage;"
        )

def stageCardsData():
    return SnowflakeOperator(
            task_id='stage-cards-data',
            snowflake_conn_id='snowflake_connection',
            sql="put file:///opt/airflow/dags/files/sd254_cards.csv @cards_stage;"
        )

def stageUserCardTransactionsData():
    return SnowflakeOperator(
            task_id='stage-user-credit-card-transactions-data',
            snowflake_conn_id='snowflake_connection',
            sql="put file:///opt/airflow/dags/files/User0_credit_card_transactions.csv @user_credit_card_transactions_stage;"
        )

with DAG('data-processor', 
        default_args=default_args,
        schedule_interval='*/60 * * * *',
        template_searchpath=['/opt/airflow/dags/queries/'],
        catchup=False) as dag:

        t1 = DummyOperator(
            task_id='start'
        )

        t2 = SnowflakeOperator(
            task_id='create-users-table',
            snowflake_conn_id='snowflake_connection',
            sql='tables/create-users-table.sql'
        )

        t3 = SnowflakeOperator(
            task_id='create-cards-table',
            snowflake_conn_id='snowflake_connection',
            sql='tables/create-cards-table.sql'
        )

        t4 = SnowflakeOperator(
            task_id='create-user-credit-card-transactions-table',
            snowflake_conn_id='snowflake_connection',
            sql='tables/create-user-credit-card-transactions-table.sql'
        )

        t5 = DummyOperator(
            task_id='tables-created'
        )

        t6 = SnowflakeOperator(
            task_id='add-users-table-primary-key',
            snowflake_conn_id='snowflake_connection',
            sql='keys/add-users-table-primary-key.sql'
        )

        t7 = SnowflakeOperator(
            task_id='add-cards-table-primary-key',
            snowflake_conn_id='snowflake_connection',
            sql='keys/add-cards-table-primary-key.sql'
        )

        t8 = SnowflakeOperator(
            task_id='add-cards-table-foreign-keys',
            snowflake_conn_id='snowflake_connection',
            sql='keys/add-cards-table-foreign-keys.sql'
        )

        t9 = SnowflakeOperator(
            task_id='add-user-credit-card-transactions-table-foreign-keys',
            snowflake_conn_id='snowflake_connection',
            sql='keys/add-user-credit-card-transactions-table-foreign-keys.sql'
        )

        t10 = SnowflakeOperator(
            task_id='create-users-stage',
            snowflake_conn_id='snowflake_connection',
            sql='stages/create-users-stage.sql'
        )

        t11 = SnowflakeOperator(
            task_id='create-cards-stage',
            snowflake_conn_id='snowflake_connection',
            sql='stages/create-cards-stage.sql'
        )

        t12 = SnowflakeOperator(
            task_id='create-user-credit-card-transactions-stage',
            snowflake_conn_id='snowflake_connection',
            sql='stages/create-user-credit-card-transactions-stage.sql'
        )

        t13 = DummyOperator(
            task_id='stages-and-keys-created'
        )

        t14 = BashOperator(
            task_id='cards-data-cleaner',
            bash_command='python3 /opt/airflow/dags/scripts/cards-data-cleaner.py')

        t15 = BashOperator(
            task_id='user-credit-card-transactions-data-cleaner',
            bash_command='python3 /opt/airflow/dags/scripts/user-credit-card-transactions-cleaner.py')

        t16 = DummyOperator(
            task_id='data-cleaners-finished'
        )

        t17 = DummyOperator(
            task_id='data-loaded-in-stages-finished'
        )

        t18 = SnowflakeOperator(
            task_id='copy-users-stage-to-table',
            snowflake_conn_id='snowflake_connection',
            sql='copy-to-table/copy-data-to-users-table.sql'
        )

        t19 = SnowflakeOperator(
            task_id='copy-cards-stage-to-table',
            snowflake_conn_id='snowflake_connection',
            sql='copy-to-table/copy-data-to-cards-table.sql'
        )

        t20 = SnowflakeOperator(
            task_id='copy-user-credit-card-transactions-stage-to-table',
            snowflake_conn_id='snowflake_connection',
            sql='copy-to-table/copy-data-to-user-credit-card-transactions-table.sql'
        )

        t1 >> [t2, t3, t4] >> t5 >> [t10, t11, t12] >> t13 >> [t14, t15] >> t16 >> [stageCardsData(), stageUserCardTransactionsData()] >> t17 >> t18 >> t19 >> t20
        t5 >> t6 >> t7 >> t9 >> t13
        t7 >> t8 >> t13
        t13 >> stageUsersData() >> t17