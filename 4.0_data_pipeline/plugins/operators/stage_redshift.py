from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 file_format='json',
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_format = file_format
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Removing data from the destination Redshift table')
        redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Copying data from S3 bucket to Redshift')
        s3_path = f's3://{self.s3_bucket}'

        if self.file_format == 'json':
            cmd = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}'" \
                f"SECRET_ACCESS_KEY '{credentials.secret_key}' JSON '{self.json_path}' COMPUPDATE OFF"
            redshift.run(sql=cmd)

        if self.file_format == 'csv':
            cmd = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}'" \
                f"SECRET_ACCESS_KEY '{credentials.secret_key}' IGNOREHEADER {self.ignore_headers}" \
                f"DELIMITER '{self.delimiter}'"
            redshift.run(sql=cmd)
