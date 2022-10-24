from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_load_param="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql_load_param=sql_load_param
        
    def execute(self, context):
  
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Populating songplays fact table')
        redshift.run(self.sql_load_param)
        
        
