from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
        Data Quality Checker
        Inputs:
            redshift_conn_id - redshift connection defined in airflow connections
            dq_checks - sql statements to run quality checks.  provided in dictionary format
                        first key 'check_sql' is sql statement to run.  Second input can either be
                        'expected_result' to compare against 'check_sql' or another sql statement,
                        'check_sql2' to compare output results against.
                        
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Begin Data Quality checks...')
        if self.dq_checks == None:
            self.log.info('No Data Quality Checks passed to operator')
            raise ValueError('No Data quality check passed')
        error_count = 0
        failing_tests = []
        for check in self.dq_checks:
            sql = check.get('check_sql')
            if 'expected_result' in check.keys():
                exp_result = check.get('expected_result')
                sql2 = None
            else:
                sql2 = check.get('check_sql2')
                exp_result = redshift.get_records(sql2)[0][0]
 
            records = redshift.get_records(sql)[0]
 
            if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)
 
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality checks have failed')
            