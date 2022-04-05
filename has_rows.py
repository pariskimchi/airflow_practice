import logging 

from airflow.hooks.postgres_hook import PostgresHook 
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults 

class HasRowsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,redshift_conn_id="",table="",*args, **kwargs):
        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.table = table 
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        # connection to redshift using PostgresHook
        redshift_hook = PostgresHook(self.redshift_conn_id)
        # count the number of records 
        records = redshift_hook.get_records(
            f"SELECT COUNT(*) FROM {self.table}"
        )

        # validate the quality 
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data Quality check failed.\
                {self.table} returned no results")
        
        num_records = records[0][0]

        if num_records < 1:
            raise ValueError(f"Data quality check failed.\
                {self.table} contained 0 rows")
        # log data 
        logging.info(f"Data quality on table {self.table} check passed with \
            {records[0][0]} records")