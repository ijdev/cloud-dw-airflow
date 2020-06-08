from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_stmt='',
                 truncate_table='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.truncate_table = truncate_table
        
        
    def execute(self, context):
        '''
        Load data to the dim table in Redshift. Provideing True to truncate will ensure delete-insert mode.
        '''
        self.log.info('Creating dims table')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table != '':
            self.log.info(f'Truncating Table {self.truncate_table}')
            redshift_hook.run("DELETE FROM {}".format(self.truncate_table))
            self.log.info(f'Truncating completed fro {self.truncate_table}')
        redshift_hook.run(self.sql_stmt)
        self.log.info('Dim table created')
