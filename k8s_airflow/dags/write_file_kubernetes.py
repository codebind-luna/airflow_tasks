import airflow
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import DAG


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='write_file_kubernetes',
    default_args=args,
    schedule_interval=None)

k = KubernetesPodOperator(namespace='default',
                          image="lunaimaginea/my_docker_file",
                          cmds=["bash", "hello.sh"],
                          name="airflow-pod-write-file",
                          in_cluster=False,
                          task_id="write_file_task",
                          get_logs=True,
                          dag=dag
                          )
