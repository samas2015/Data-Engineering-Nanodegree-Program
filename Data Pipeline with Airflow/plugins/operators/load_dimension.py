from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_load_param="",
                 append="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.sql_load_param=sql_load_param
        self.append=append
        self.table=table
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append==False:
            self.log.info("Clearing data from {} destination Redshift table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info('Populating dimentsion table')
        redshift.run(self.sql_load_param)
      
