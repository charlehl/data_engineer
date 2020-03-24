from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
        Load Dimension Table
        Inputs:
            redshift_conn_id - redshift connection defined in airflow connections
            dim_table - name of dimesion table to load in Redshift
            sql_statement - Contains sql code to create table used with sql_load_template to load data into dim_table.
            append_data - Boolean used to either append data to existing tables if True and create new table if False.
    """
    sql_load_template = """
        INSERT INTO {}
        {}
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table
        self.sql_statement=sql_statement
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.dim_table))
        
        self.log.info('Load Dimension Table {}'.format(self.dim_table))
        sql_dim_statement = LoadDimensionOperator.sql_load_template.format(
            self.dim_table,
            self.sql_statement
        )
        redshift.run(sql_dim_statement)
