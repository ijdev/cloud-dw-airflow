
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        DELIMITER '{}'
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        ;
    """
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
        ;
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials='',
                 s3_bucket='',
                 s3_key='',
                 table='',
                 deli='',
                 json='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.deli = deli
        self.json = json

    def execute(self, context):
        '''
        Create a connection to AWS using the aws_credentials and create a PostgresHook to connect to Amazon Redshift.
        Get the data from s3 bucket and load it to redshift tables

        '''
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #execution_date = kwargs["execution_date"]

        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Copying data to {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = ''
        if self.json == '':
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.deli
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json
            )
        redshift_hook.run(formatted_sql)
        self.log.info(f"Copying data to {self.table} completed")








#
#
# COPY tablename
# FROM 'data_source'
# CREDENTIALS 'credentials-args'
# FORMAT AS { AVRO | JSON } 's3://jsonpaths_file';