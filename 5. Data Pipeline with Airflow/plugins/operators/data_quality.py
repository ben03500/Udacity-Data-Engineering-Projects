from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operator import eq, ne, gt, lt, le, ge

ops = {"=": eq, "!=": ne, ">": gt, "<": lt, "<=": le, ">=": ge}


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info("Connecting to Redshift cluster...")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.dq_checks:
            check_sql = check["check_sql"]
            self.log.info(f"Performing data quality check with '{check_sql}'")
            result = redshift_hook.get_records(check_sql)

            if ops[check["comparison"]](result[0][0], check["expected_result"]):
                self.log.info("Data quality check passed.")
            else:
                self.log.error("Data quality check failed.")
                raise ValueError("Data quality check failed.")
