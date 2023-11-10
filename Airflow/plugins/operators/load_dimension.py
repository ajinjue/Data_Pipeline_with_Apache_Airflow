#from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {};
    """
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 append_mode = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql,
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('Connecting to Redshift to Insert Data')
        redshift_hook = PostgresHook("redshift")
        
        if self.append_mode:
            self.log.info(f"Inserting Data to {self.table} ")
            load_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.sql
            )
            redshift_hook.run(load_sql)
        else:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.table))
        
