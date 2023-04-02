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

with dag:

    config_dir = f"{pathlib.Path(__file__).resolve().parent}/config"

    entry = DummyOperator(
        task_id="entry",
    )

    exit = DummyOperator(
        task_id="exit",
    )

    with TaskGroup(group_id=f"datalake"):
        with open(f"{config_dir}/datalake.yml", "r") as yml:
            datalake_config = yaml.load(yml)

        datalake_entry = DummyOperator(
            task_id="datalake-entry",
        )

        datalake_exit = DummyOperator(
            task_id="datalake-exit",
        )

        for domain in datalake_config.get("domains"):

            with TaskGroup(group_id=f"datalake-{domain.get('name')}"):
                datalake_system_entry = DummyOperator(
                    task_id=f"datalake-{domain.get('name')}-entry",
                )

                datalake_system_exit = DummyOperator(
                    task_id=f"datalake-{domain.get('name')}-exit",
                )
                for table in domain.get("tables"):
                    datalake_embulk_run = SSHOperator(
                        task_id=f"datalake.{domain.get('name')}.{table.get('name')}",
                        ssh_conn_id="odp_java_ssh",
                        command=" && ".join([
                            f"cd /var/apps/embulk",
                            " ".join([
                                f"embulk",
                                f"run",
                                f"config/datalake.{domain.get('name')}.{table.get('name')}.yml.liquid",
                                f"--bundle bundle",
                                f"--log-level error",
                            ])
                        ]),
                    )
                    datalake_system_entry >> datalake_embulk_run
                    datalake_embulk_run >> datalake_system_exit

            datalake_entry >> datalake_system_entry
            datalake_system_exit >> datalake_exit

    with TaskGroup(group_id=f"datawarehouse"):

        with open(f"{config_dir}/datawarehouse.yml", "r") as yml:
            datawarehouse_config = yaml.load(yml)

        datawarehouse_entry = DummyOperator(
            task_id="datawarehouse-entry",
        )

        datawarehouse_exit = DummyOperator(
            task_id="datawarehouse-exit",
        )

        for domain in datawarehouse_config.get("domains"):

            with TaskGroup(group_id=f"domain-{domain.get('name')}"):
                datawarehouse_domain_entry = DummyOperator(
                    task_id=f"datawarehouse-{domain.get('name')}-entry",
                )

                datawarehouse_domain_exit = DummyOperator(
                    task_id=f"datawarehouse-{domain.get('name')}-exit",
                )
                for table in domain.get("tables"):
                    datawarehouse_embulk_run = SSHOperator(
                        task_id=f"domain.{domain.get('name')}.{table.get('name')}",
                        ssh_conn_id="odp_java_ssh",
                        command=" && ".join([
                            f"cd /var/apps/embulk",
                            " ".join([
                                f"embulk",
                                f"run",
                                f"config/datawarehouse.{domain.get('name')}.{table.get('name')}.yml.liquid",
                                f"--bundle bundle",
                                f"--log-level error",
                            ])
                        ]),
                    )
                    datawarehouse_domain_entry >> datawarehouse_embulk_run
                    datawarehouse_embulk_run >> datawarehouse_domain_exit

            datawarehouse_entry >> datawarehouse_domain_entry
            datawarehouse_domain_exit >> datawarehouse_exit


    with TaskGroup(group_id=f"datamart"):
        with open(f"{config_dir}/datamart.yml", "r") as yml:
            datamart_config = yaml.load(yml)

        datamart_entry = DummyOperator(
            task_id="datamart-entry",
        )

        datamart_exit = DummyOperator(
            task_id="datamart-exit",
        )

        for domain in datamart_config.get("domains"):

            with TaskGroup(group_id=f"datamart-{domain.get('name')}"):
                datamart_domain_entry = DummyOperator(
                    task_id=f"datamart-{domain.get('name')}-entry",
                )

                datamart_domain_exit = DummyOperator(
                    task_id=f"datamart-{domain.get('name')}-exit",
                )
                for table in domain.get("tables"):
                    datamart_embulk_run = SSHOperator(
                        task_id=f"datamart.{domain.get('name')}.{table.get('name')}",
                        ssh_conn_id="odp_java_ssh",
                        command=" && ".join([
                            f"cd /var/apps/embulk",
                            " ".join([
                                f"embulk",
                                f"run",
                                f"config/datamart.{domain.get('name')}.{table.get('name')}.yml.liquid",
                                f"--bundle bundle",
                                f"--log-level error",
                            ])
                        ]),
                    )
                    datamart_domain_entry >> datamart_embulk_run
                    datamart_embulk_run >> datamart_domain_exit

            datamart_entry >> datamart_domain_entry
            datamart_domain_exit >> datamart_exit

    entry >> datalake_entry
    datalake_exit >> datawarehouse_entry
    datawarehouse_exit >> datamart_entry
    datamart_exit >> exit
