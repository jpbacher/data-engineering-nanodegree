from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 truncate='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading dimension table: {self.table}')
        if self.truncate:
            cmd = f"""
                BEGIN;
                TRUNCATE TABLE {self.table};
                INSERT INTO {self.table} {self.sql_query};
                COMMIT;
                """
        else:
            cmd = f"""
                BEGIN;
                INSERT INTO {self.table} {self.sql_query};
                COMMIT;
                """

        redshift.run(sql=cmd)
        self.log.info(f'Success: {self.task_id}')
