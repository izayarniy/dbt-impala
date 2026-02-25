import pytest
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    run_dbt,
    write_file,
)


input_model_sql = """
{{ config(materialized='table',
          table_type='iceberg',
          file_format='parquet',
          tblproperties={
          'write.format.default': 'PARQUET',
          'table_description': 'My Iceberg Table'}
         )
    
 }}
select cast(1 as INT) as id, 'CT' as state,  to_timestamp('2020-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as event_time
union all
select cast(2 as INT) as id, 'MA' as state, to_timestamp('2020-01-02 00:00:00', 'yyyy-MM-dd HH:mm:ss') as event_time
union all
select cast(3 as INT) as id, 'NJ' as state, to_timestamp('2020-01-03 00:00:00', 'yyyy-MM-dd HH:mm:ss') as event_time
"""


incremental_model_sql = """
{{ config(materialized='incremental',
          incremental_strategy='append',
          table_type='iceberg',
          file_format='parquet',
          tblproperties={
          'write.format.default': 'PARQUET',
          'table_description': 'My Iceberg Table'}
          ) 
}}

SELECT
    id, 
    state, 
    event_time
FROM {{ ref('input_model') }}

{% if is_incremental() %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
"""

shema_yml ="""

"""
class TestMergeStrategy:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": input_model_sql,
            "incremental_model.sql": incremental_model_sql,
        }



    def tests_merge_support(self, project):
        # model_result_set = run_dbt(["run", "--select", "base_incremental_model"])
        # run initial state models.
        result = run_dbt(["run"])
        # insert update into input model
        # test_schema_relation = project.adapter.Relation.create(
        #     database=project.database, schema=project.test_schema
        # )
        target_db = project.created_schemas[0]
        insert_new_values = f"""
  insert into 
  {target_db}.input_model 
  (id, state, event_time) values
  (4, 'MA', to_timestamp('2020-02-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') ),
  (5,'MA', to_timestamp('2020-02-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') )
        """
        project.run_sql(insert_new_values)
        result = run_dbt(["run", "--select", "incremental_model"], True)
        print(result)

