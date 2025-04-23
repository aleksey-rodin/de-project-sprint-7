import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"


default_args = {
    'owner': 'airflow',
    'start_date':datetime(2025, 4, 20),
}

dag = DAG(
    dag_id = "dm_processing",
    default_args=default_args,
    schedule_interval=None
)

t1 = SparkSubmitOperator(
    task_id="processing_dm_users",
    dag=dag,
    application="/lessons/dm_users.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "10",
        "/user/rodinas/data/geo.csv",
        "/user/master/data/geo/events",
        "/user/rodinas/data/analytics/datamarts/dm_users",
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_memory="2g",
    executor_cores=2,
)

t2 = SparkSubmitOperator(
    task_id="processing_dm_zones",
    dag=dag,
    application="/lessons/dm_zones.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "10",
        "/user/rodinas/data/geo.csv",
        "/user/master/data/geo/events",
        "/user/xeniakutse/data/analytics/dm_zones",
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_memory="2g",
    executor_cores=2,
)

t3 = SparkSubmitOperator(
    task_id="processing_dm_recommendations",
    dag=dag,
    application="/lessons/dm_recommendations.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "10",
        "/user/rodinas/data/geo.csv",
        "/user/master/data/geo/events",
        "/user/xeniakutse/data/analytics/dm_recommendations",
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_memory="2g",
    executor_cores=2,
)

t1 >> t2 >> t3
