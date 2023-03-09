from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import telegram_bot as telegram_bot
import jira_report as jira_report
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.email_operator import EmailOperator

# 1. Get data from Jira ✅
# 2. Send notification with statistic info to telegram ✅
# 3. Save data to database to caculate rank ✅
# 4. Send current rank

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5),
    'email': ['20158367@sie.edu.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def upload_mongo(**context):
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.airflow
    report_job = db.report_job
    print(f"Connected to MongoDB - {client.server_info()}")
    ti = context['ti']
    data_list = ti.xcom_pull(key='return_value')
    report_job.insert_many(data_list)


def get_report_jira():
    report = jira_report.get_report()
    return report


def send_message_telegram(**context):
    ti = context['ti']
    reports = ti.xcom_pull(key='return_value')
    for email, listIssues in reports.items():
        telegram_bot.sendMessage(
            f"Chúc mừng 🤩🤩 {email} đã hoàn thành {len(listIssues)} công việc\n {', '.join(listIssues)}")


def send_start_notification():
    message = f"Báo cáo kết quả cuối ngày 👈👈👈"
    telegram_bot.sendMessage(message)


def transform_data(**context):
    ti = context['ti']
    data_list = ti.xcom_pull(key='return_value')
    list_email = []
    current_time = datetime.now()
    print("current_time", current_time)
    for email, listIssues in data_list.items():
        list_email.append({
            'name': email,
            'listIssues': listIssues,
            'count': len(listIssues),
            'createdAt': str(current_time)
        })
    return list_email


dag = DAG('reminder_dag', default_args=default_args,
          schedule_interval=timedelta(days=1))

# Define tasks

get_report = PythonOperator(
    task_id='get_report',
    python_callable=get_report_jira,
    dag=dag
)

send_telegram = PythonOperator(
    task_id='send_telegram',
    python_callable=send_message_telegram,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=transform_data,
    dag=dag
)

save_data = PythonOperator(
    task_id='save_data',
    python_callable=upload_mongo,
    dag=dag
)

start_noti = PythonOperator(
    task_id="send_start_notification",
    python_callable=send_start_notification,
    dag=dag
)

send_email_notification = EmailOperator(
    task_id="send_email_notification",
    to="20158367@sie.edu.vn",
    subject="Test airflow",
    html_content="<h3>data</h3>",
    dag=dag
)

# Define task dependencies
start_noti >> get_report >> send_telegram >> send_email_notification
get_report >> process_data >> save_data
