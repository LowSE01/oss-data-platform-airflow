import datetime
import pathlib
import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'on_failure_callback': None,
    'on_success_callback': None,
    'owner': 'airflow',
    'retries': int(Variable.get("daily_dag_default_args_retries")),
    'start_date': datetime.datetime(year=2023, month=3, day=28, hour=0, minute=0),
}

dag_doc_md = f"""
# Daily Pipeline
"""

dag = DAG(
    catchup=False,
    concurrency=int(Variable.get("daily_dag_concurrency")),
    dag_id="daily-pipeline-with-embulk",
    default_args=default_args,
    doc_md=dag_doc_md,
    schedule_interval=Variable.get("daily_dag_schedule_interval"),
)

class Layer:

    def __init__(self, layer_name):

        with open(f"{pathlib.Path(__file__).resolve().parent}/config/{layer_name}.yml", "r") as yml:
            config = yaml.load(yml)

        with TaskGroup(group_id=f"{layer_name}"):
            self.entry = DummyOperator(
                task_id=f"{layer_name}-entry",
            )
            self.exit = DummyOperator(
                task_id=f"{layer_name}-exit",
            )

            for domain in config.get("domains"):
                with TaskGroup(group_id=f"{layer_name}-{domain.get('name')}"):
                    domain_entry = DummyOperator(
                        task_id=f"{layer_name}-{domain.get('name')}-entry",
                    )
                    domain_exit = DummyOperator(
                        task_id=f"{layer_name}-{domain.get('name')}-exit",
                    )

                    for table in domain.get("tables"):
                        embulk = SSHOperator(
                            task_id=f"{layer_name}.{domain.get('name')}.{table.get('name')}",
                            ssh_conn_id="odp_java_ssh",
                            command=" && ".join([
                                f"cd /var/apps/embulk",
                                " ".join([
                                    f"embulk",
                                    f"run",
                                    f"config/{layer_name}.{domain.get('name')}.{table.get('name')}.yml.liquid",
                                    f"--bundle bundle",
                                    f"--log-level error",
                                ])
                            ]),
                        )
                        domain_entry >> embulk >> domain_exit

                    self.entry >> domain_entry
                    domain_exit >> self.exit

with dag:

    entry = DummyOperator(task_id="entry")
    exit = DummyOperator(task_id="exit")

    with TaskGroup(group_id=f"Extract-and-Load"):
        datalake        = Layer("datalake")

    with TaskGroup(group_id=f"Transform"):
        datawarehouse   = Layer("datawarehouse")
        datamart        = Layer("datamart")

    entry >> datalake.entry
    datalake.exit >> datawarehouse.entry
    datawarehouse.exit >> datamart.entry
    datamart.exit >> exit
