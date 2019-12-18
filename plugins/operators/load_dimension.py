from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
"""
With dimension and fact operators, you can utilize the provided SQL helper class
 to run data transformations. Most of the logic is within the SQL transformations
 and the operator is expected to take as input a SQL statement and target database
 on which to run the query against. 
 
    :param redshift_conn_id: AWS Redshift connection ID
    :param tables: SQL tables passed in sql_queries.py
	:param load_sql_stmt: load stament for table
    :param truncate: truncate/insert vs only sert into dimension table
"""

    ui_color = '#509F23'
    
    
    @apply_defaults
    def __init__(self,
                 , redshift_conn_id=""
                 , tables=""
                 , load_sql_stmt=""
                 , truncate=False
                 , *args
                 , **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('LoadDimensionOperator executing')
        
        if self.truncate:
            pg_hook.run(f"TRUNCATE TABLE {self.tables}")
        
        formatted_sql = f"INSERT INTO {self.table} (self.load_sql_stmt})
        pg_hook.run(formatted_sql)
            