from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ The operator's main functionality is to receive one or more SQL based test
    cases along with the expected results and execute the tests. For each the test,
    the test result and expected result needs to be checked and if there is no match,
    the operator should raise an exception and the task should retry and fail eventually.

    :param redshift_conn_id: AWS Redshift connection ID
    :param tables: list of SQL tables passed in sql_queries.py
    """
    ui_color = '#509F23'

    @apply_defaults
    def __init__(self,
                 , redshift_conn_id=""
                 , tables=[]
                 , *args
                 , **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info("DataQualityOperator getting Credentials")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            self.log.info('DataQualityOperator executing for: {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Quality failed. {table}, no data")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Quality failed. {table}, no data")

            self.log.info("Data quality passed {table} with {records[0][0]} records")