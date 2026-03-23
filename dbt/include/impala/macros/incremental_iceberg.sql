{% macro impala_iceberg_iceberg_validate_get_incremental_strategy(incremental_strategy) %}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ incremental_strategy }}
    Expected one of: 'merge'
  {%- endset %}

  {% if incremental_strategy not in ['merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}
  {% do return(incremental_strategy) %}
{% endmacro %}


{% macro impala_iceberg_incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
   {% if on_schema_change not in ['fail', 'ignore'] %}
     {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message) %}

     {% do exceptions.raise_compiler_error(log_message) %}

     {{ return(default) }}
   {% else %}
     {{ return(on_schema_change) }}
   {% endif %}
{% endmacro %}


{% materialization incremental_iceberg, adapter='impala' -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  -- configs
  {% set unique_key = config.get('unique_key') %}
  {% set incremental_strategy = config.get('incremental_strategy') or 'merge' %}
  {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {% if incremental_strategy == None %}
    {% set incremental_strategy = 'merge' %}
  {% endif %}
  {% set incremental_strategy = impala_iceberg_iceberg_validate_get_incremental_strategy(incremental_strategy) %}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {% set on_schema_change = impala_iceberg_incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  -- log incremental strategy
  {% do target_relation.log_relation(incremental_strategy) %}
  {% set grant_config = config.get('grants') %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {% if incremental_strategy == 'merge' %}
       {{ log("Preparing insert overwrite for full refresh mode. Supported only over iceberg v2 format." ~ incremental_strategy, info=True) }}
       {% set build_sql = impala_iceberg_built_insert_overwrite_sql(target_relation, sql)%}
      {% endif %}
  {% else %}
    {{ drop_relation_if_exists(temp_relation) }}
    {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}
    {#
    dbt.expand_target_column_types is a backend adapter function used in dbt materializations (often incremental) to automatically resize string columns
    (e.g., varchar(3) to varchar(6)) in the target table to match new, larger incoming data.
    It is primarily handled internally to prevent schema mismatch errors during incremental runs, rather than invoked by users.
    #}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = impala_iceberg__get_merge_sql(target_relation, temp_relation, unique_key, dest_columns, incremental_predicates) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

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

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro impala_iceberg_built_insert_overwrite_sql(target, sql_code) -%}
    INSERT OVERWRITE TABLE  {{ target }}
    {{ sql_code}}
{%- endmacro %}

{% macro impala_iceberg__get_merge_sql(target,
                               source,
                               unique_key,
                               dest_columns,
                               incremental_predicates,
                               merge_update_columns,
                               merge_exclude_columns) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set dest_cols_csv_source = dest_cols_csv.split(', ') -%}
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
