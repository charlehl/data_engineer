from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class LoadFactOperator(BaseOperator):
    """
        Load Fact Table
        Inputs:
            redshift_conn_id - redshift connection defined in airflow connections
            fact_table - name of fact table to load in Redshift
            sql_statement - Contains sql code to create table used with sql_load_template to load data into fact_table.
            append_data - Boolean used to either append data to existing tables if True and create new table if False.
    """
    sql_load_template = """
        INSERT INTO {}
        {}
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 sql_statement="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.sql_statement=sql_statement
        self.append_data = append_data
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.fact_table))
        
        self.log.info('Load Fact Table {}'.format(self.fact_table))
        sql_fact_statement = LoadFactOperator.sql_load_template.format(
            self.fact_table,
            self.sql_statement
        )
        redshift.run(sql_fact_statement)
