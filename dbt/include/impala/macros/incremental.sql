{#
# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#}

{% macro validate_get_incremental_strategy(incremental_strategy) %}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ incremental_strategy }}
    Expected one of: 'append', 'insert_overwrite', 'microbatch', 'merge'
  {%- endset %}

  {% if incremental_strategy not in ['append', 'insert_overwrite', 'microbatch', 'merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% if incremental_strategy == 'microbatch' %}
    {{ validate_partition_key_for_microbatch_strategy() }}
  {%- endif -%}

  {% do return(incremental_strategy) %}
{% endmacro %}

{% macro validate_partition_key_for_microbatch_strategy() %}
    {% set microbatch_partition_key_missing_msg -%}
      dbt-impala 'microbatch' incremental strategy requires a `partition_by` config.
      Ensure you are using a `partition_by` column that is of granularity {{ config.get('batch_size') }}.
    {%- endset %}

    {%- if not config.get('partition_by') -%}
      {{ exceptions.raise_compiler_error(microbatch_partition_key_missing_msg) }}
    {%- endif -%}
{% endmacro %}

{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
   {% if on_schema_change not in ['fail', 'ignore'] %}
     {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message) %}

     {% do exceptions.raise_compiler_error(log_message) %}

     {{ return(default) }}
   {% else %}
     {{ return(on_schema_change) }}
   {% endif %}
{% endmacro %}

{% macro impala__get_incremental_default_sql(arg_dict) %}
   {% do return(get_insert_overwrite_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}
{% endmacro %}

{% materialization incremental, adapter='impala' -%}
--  # TODO: for merge materialization strategy should check table format. supported only for iceberg_v2
  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- configs
  {% set unique_key = config.get('unique_key') %}
  {% set incremental_strategy = config.get('incremental_strategy') or 'append' %}
  {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
  {% if incremental_strategy == None %}
    {% set incremental_strategy = 'append' %}
  {% endif %}
  {% set incremental_strategy = validate_get_incremental_strategy(incremental_strategy) %}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  -- log incremental strategy
  {% do target_relation.log_relation(incremental_strategy) %}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {% if incremental_strategy == 'merge' %}
       {{ log("Preparing insert overwrite for full refresh mode. Supported only over iceberg v2 format." ~ incremental_strategy, info=True) }}
       {% set build_sql = impala_built_insert_overwrite_sql(target_relation, sql)%}
       {% set need_swap = false %}
      {% else %}
        {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
      {% endif %}
  {% else %}
    {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}
    {% do to_drop.append(temp_relation) %}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_predicates = config.get('incremental_predicates', none) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation,
                                 'temp_relation': temp_relation,
                                 'unique_key': unique_key,
                                 'dest_columns': dest_columns,
                                 'incremental_predicates': incremental_predicates }) %}
    {% if incremental_strategy == 'merge' %}
        {% set build_sql = adapter.get_incremental_strategy_macro(context, incremental_strategy)(strategy_arg_dict) %}
    {% else %}
        {% set build_sql = get_incremental_default_sql(strategy_arg_dict) %}
    {% endif %}
  {% endif %}


  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro impala_built_insert_overwrite_sql(target, sql_code) -%}
    INSERT OVERWRITE TABLE  {{ target }}
    {{ sql_code}}
{%- endmacro %}

{% macro impala__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set dest_cols_csv_source = dest_cols_csv.split(', ') -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}

        {{ sql_header if sql_header is not none }}

        merge into {{ target }} as DBT_INTERNAL_DEST
            using {{ source }} as DBT_INTERNAL_SOURCE
            on {{"(" ~ predicates | join(") and (") ~ ")"}}

        {% if unique_key %}
        when matched then update set
            {% for column_name in update_columns -%}
                {{ column_name | replace('"', "`") }} = DBT_INTERNAL_SOURCE.{{ column_name | replace('"', "`") }}
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
        {% endif %}

        when not matched then insert
            ({{ dest_cols_csv }})
        values
            ({% for dest_cols in dest_cols_csv_source -%}
                DBT_INTERNAL_SOURCE.{{ dest_cols }}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %})

    {% else %}
        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% endif %}
{%- endmacro %}
