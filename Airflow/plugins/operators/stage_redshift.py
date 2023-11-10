#from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        REGION AS '{}'
        FORMAT AS JSON '{}'   
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket= "",
                 s3_key = "",
                 ignore_headers=1,
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params 
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers= ignore_headers
        self.region = region
    def execute(self, context):
        self.log.info('Connecting to AWS and Redshift')
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.region
        )
        self.log.info(f" Copying data from '{s3_path}' to '{self.table}'")
        redshift_hook.run(formatted_sql)












