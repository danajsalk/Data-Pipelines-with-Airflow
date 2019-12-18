from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
"""
With dimension and fact operators, you can utilize the provided SQL helper class
to run data transformations. Most of the logic is within the SQL transformations
and the operator is expected to take as input a SQL statement and target database
on which to run the query against.
 
    :param redshift_conn_id: AWS Redshift connection ID
    :param table: SQL tables passed in sql_queries.py
	:param sql_stmt: SQL query to load data
"""
    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 , redshift_conn_id=""
                 , table=""
                 , sql_stmt=""
                 , *args
                 , **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('LoadFactOperator executing')
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_stmt
        )
        redshift.run(formatted_sql)
