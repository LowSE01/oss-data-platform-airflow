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
    dag_id="daily-pipeline-with-dbt",
    default_args=default_args,
    doc_md=dag_doc_md,
    schedule_interval=Variable.get("daily_dag_schedule_interval"),
)

class LayerBase:

    def ssh_conn_id(self):
        return None

    def command(self, layer_name, domain_name, table_name):
        return None

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
                            ssh_conn_id=self.ssh_conn_id(),
                            command=self.command(layer_name, domain.get("name"), table.get("name"))
                        )
                        domain_entry >> embulk >> domain_exit

                    self.entry >> domain_entry
                    domain_exit >> self.exit

class LayerEmbulk(LayerBase):
    def ssh_conn_id(self):
        return "odp_java_ssh"

    def command(self, layer_name, domain_name, table_name):
        return " && ".join([
            f"cd /var/apps/embulk",
            " ".join([
                f"embulk",
                f"run",
                f"config/{layer_name}.{domain_name}.{table_name}.yml.liquid",
                f"--bundle bundle",
                f"--log-level error",
            ])
        ])

class LayerDbt(LayerBase):
    def ssh_conn_id(self):
        return "odp_python3_ssh"

    def command(self, layer_name, domain_name, table_name):
        return " && ".join([
            f"cd /var/apps/dbt",
            " ".join([
                f"dbt",
                f"run",
                f"--select {layer_name}.{domain_name}_{table_name}",
            ])
        ])

with dag:

    entry = DummyOperator(task_id="entry")
    exit = DummyOperator(task_id="exit")

    with TaskGroup(group_id=f"Extract-and-Load"):
        datalake        = LayerEmbulk("datalake")

    with TaskGroup(group_id=f"Transform"):
        datawarehouse   = LayerDbt("datawarehouse")

    entry >> datalake.entry
    datalake.exit >> datawarehouse.entry
    datawarehouse.exit >> exit
