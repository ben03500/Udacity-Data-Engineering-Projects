from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 mode="append-only",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.mode = mode

    def execute(self, context):
        self.log.info('Connecting to Redshift cluster...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == "delete-load":
            self.log.info(f"Begin deleting data from {self.table}")
            redshift.run("TRUNCATE {}".format(self.table))
        else:
            pass

        self.log.info("Inserting data to Redshift dimension table...")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))

        self.log.info("Completed data insertion.")
