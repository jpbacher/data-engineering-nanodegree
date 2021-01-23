from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self. sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgress_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading fact table: {self.table}')
        cmd = f"INSERT INTO {self.table} {self.sql_query} COMMIT"
        redshift.run(sql=cmd)
