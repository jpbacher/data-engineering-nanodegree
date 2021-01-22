from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')





