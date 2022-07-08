from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table_name="",
                load_sql="",
                append_only=False,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.load_sql = load_sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into {self.table_name} dimension table")
        self.log.info(f"Append only: {self.append_only}")
        if self.append_only:
            self.log.info(f"Appending to {self.table_name}")
            redshift.run(f"INSERT INTO {self.table_name} {self.load_sql}")
        else:
            self.log.info(f"Truncating {self.table_name}")
            redshift.run(f"DELETE FROM {self.table_name}")
            self.log.info(f"Loading data into {self.table_name}")
            redshift.run(f"INSERT INTO {self.table_name} {self.load_sql}")
