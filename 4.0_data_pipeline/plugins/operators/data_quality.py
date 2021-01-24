from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f'Inspecting record count of {table} table')
            cmd = f'SELECT COUNT(*) FROM {table}'
            records = redshift_hook.get_records(sql=cmd)
            if len(records[0] < 1) or len(records) < 1:
                self.log.error(f'Data quality check failed...{table} did not return any results')
                raise ValueError(f'Data quality check failed...{table} did not return any results')
            record_count = records[0][0]
            if record_count < 1:
                self.log.error(f'Data quality check failed...{table} has 0 rows')
                raise ValueError(f'Data quality check failed...{table} has 0 rows')
            self.log.info(f'Data quality check passed - {table} table: {record_count} records')
