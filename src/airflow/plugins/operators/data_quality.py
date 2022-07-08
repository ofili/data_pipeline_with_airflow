from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table_names=None,
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if table_names is None:
            table_names = []
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.table_names:
            # check if entries are copied to staging table 
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        self.log.info("Data quality check passed")
        # return True

        # check that there are no null values in the columns
        dq_checks = [
            {'sql': f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL", 'expected_result': 0}
            for table, column in self.column_null_checks.items()
        ]
        for check in dq_checks:
            records = redshift.get_records(check['sql'])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {check['sql']} returned no results")
            num_records = records[0][0]
            if num_records != check['expected_result']:
                raise ValueError(
                    f"Data quality check failed. {check['sql']} contained {records[0][0]} records, expected {check['expected_result']}")
            self.log.info(f"Data quality check passed. {check['sql']} contained {records[0][0]} records")
        self.log.info("Data quality check passed")
        return True
