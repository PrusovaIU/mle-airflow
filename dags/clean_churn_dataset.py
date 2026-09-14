# DAG airflow
# dags/churn.py

import pendulum
import pandas as pd
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"]
)
def clean_churn_dataset():
    
    @task()
    def create_table():
        from sqlalchemy import (
            MetaData, Table, Column, Integer, String,
            DateTime, Float, UniqueConstraint
        )
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        Table(
            'clean_users_churn',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('customer_id', String),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', String),
            Column('dependents', String),
            Column('multiple_lines', String),
            Column('target', Integer),
            UniqueConstraint('customer_id', name='clean_users_churn_unique_customer_id'),
        )
        metadata.create_all(conn)

    @task()
    def extract():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = "select * from users_churn;"
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        # удаление дубликатов
        feature_cols = list(data.drop(columns=['customer_id']).columns)
        is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
        data = data[~is_duplicated_features].copy()

        # отсев выбросов по методу IQR
        num_cols = data.select_dtypes(['float']).columns
        threshold = 1.5
        potential_outliers = pd.DataFrame()

        for col in num_cols:
            Q1 = data[col].quantile(0.25)
            Q3 = data[col].quantile(0.75)
            IQR = Q3 - Q1
            margin = threshold * IQR
            lower = Q1 - margin
            upper = Q3 + margin
            potential_outliers[col] = ~data[col].between(lower, upper)

        outliers = potential_outliers.any(axis=1)
        data = data[~outliers]

        # заполнение пропусков
        cols_with_nans = data.isnull().sum()
        cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')

        for col in cols_with_nans:
            if data[col].dtype in [float, int]:
                fill_value = data[col].mean()
            elif data[col].dtype == 'object':
                fill_value = data[col].mode()[0]
            data[col] = data[col].fillna(fill_value)

        return data

    @task()
    def load(data: pd.DataFrame):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook('destination_db')
        for col in data.select_dtypes(include=['datetime64[ns]']).columns:
            data[col] = data[col].astype(object).where(data[col].notna(), None)
        hook.insert_rows(
            table="clean_users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist()
        )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_churn_dataset()