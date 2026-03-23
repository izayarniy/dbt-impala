import pytest
from dbt.tests.util import (
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    run_dbt,
    write_file,
)

simple_model = """
{{ config(materialized='table',
   persist_docs={"relation": false,
                 "columns": true}
   ) }}

with source_data as (
    select 1 as id,
           'Ivan' as first_name
    union all
    select 2 as id,
           'Anna' as first_name
)


select *
from source_data
"""

schema = """
version: 2
models:
  - name: simple_model
    description: "bla bla bla"
    columns: &summary_columns
      - name: id
        description: "fru fru fru"
      - name: first_name
        description: "My first name"
"""


class TestColumnCommentsInModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_model.sql": simple_model, "schema.yml": schema}

    def test_persist_docs(self, project):
        # run models
        run_dbt(["run"])
        print("fruu")
