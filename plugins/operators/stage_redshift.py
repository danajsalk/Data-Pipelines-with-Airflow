from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
"""
The stage operator loads any JSON formatted file from S3 to Amazon 
Redshift. The operator creates and runs a SQL COPY statement 
based on the parameters provided. The operator's parameters specify 
where in S3 the file is loaded and what is the target table.

    :param redshift_conn_id: AWS Redshift connection ID
	:param aws_credentials_id: AWS Stored Credentials
    :param table: list of SQL tables passed in sql_queries.py
	:param s3_bucket: AWS S3 bucket
	:param s3key: AWS S3 Key
	:param file_type: data source format
	:param ignore_headers: ignore headers in CSV

"""
    ui_color = '#358140'

    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        COMPUPDATE OFF
    """

    csv_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
    """

    @apply_defaults
    def __init__(self,
                 , redshift_conn_id=""
                 , aws_credentials_id=""
                 , table=""
                 , s3_bucket=""
                 , s3_key=""
                 , file_type=""
                 , ignore_headers=1
                 , *args
                 , **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.aws_credentials_id = aws_credentials_id
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info("stage_redshift getting Credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('StageToRedshiftOperator executing')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.file_type == "json":
            self.log.info('StageToRedshiftOperator: {table}: json')
            formatted_sql = StageToRedshiftOperator.json_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
            )
            redshift.run(formatted_sql)

        else self.file_type == "csv":
            self.log.info('StageToRedshiftOperator: {table}: csv')
            formatted_sql = StageToRedshiftOperator.csv_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
            )
            redshift.run(formatted_sql)





