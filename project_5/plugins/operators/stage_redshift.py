from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
        Load Staging Table
        Inputs:
            redshift_conn_id - redshift connection defined in airflow connections
            aws_credentials_id - aws credentials defined in airflow connections
            table - name of staging table to load in Redshift
            s3_bucket - s3 bucket to load data from
            s3_key - s3 key to load data from
            region - region of s3 bucket
            json_path - json path file or use auto
            execution_date - execution date if loading by year/month from S3 bucket
            append_data - Boolean used to either append data to existing tables if True and create new table if False.
    """
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 json_path="auto",
                 execution_date = '',
                 append_data = False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path
        self.execution_date = execution_date
        self.append_data = append_data

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        exec_date = context.get("execution_date")
        self.log.info("Execution_date is: {}".format(exec_date.strftime("%m/%d/%y")))

        if self.append_data == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info("Rendered s3 key is: {}".format(rendered_key))
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info("Copying data from S3 to Redshift Complete")
