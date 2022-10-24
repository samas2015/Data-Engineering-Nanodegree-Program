from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                  redshift_conn_id="",
                 tables="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables=tables
        self.dq_checks=dq_checks
        
    def execute(self, context):
      
        self.log.info('Data Quality Check is running ')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        tables_param = kwargs["tables"]["table"]
        sql_stmt=kwargs["dq_checks"]["check_sql"]
        expected_result=kwargs["dq_checks"]["expected_result"]
        
        for table in  tables_param:
            
            records = redshift_hook.get_records(sql_stmt+table)
            if len(records) < expected_result or len(records[0]) < expected_result:
             raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < expected_result:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")