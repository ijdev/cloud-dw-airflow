from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        '''
        A data quilaty check based on the list of dictionary provided.
        If the check fails an error will be thrown.
        '''
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        fails = []
        error_count = 0
        for check in self.data_quality_checks:
            sql = check.get('sql')
            exp_result = check.get('exp')

            records = redshift_hook.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                fails.append(sql)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(fails)
            raise ValueError(f'Data quality check failed')

        self.log.info(f"Data quilaty checks passed")
