with
    source as (select * from {{ ref("stages_yaml_source") }}),
    groups_exploded as (

        select
            {{
                dbt_utils.star(
                    from=ref("stages_yaml_source"), except=["STAGE_GROUPS"]
                )
            }}, d.value as data_by_row
        from source, lateral flatten(input => parse_json(stage_groups::variant)[0]) d

    ),
    groups_parsed_out as (

        select
            {{
                dbt_utils.star(
                    from=ref("stages_yaml_source"), except=["STAGE_GROUPS"]
                )
            }},
            data_by_row['name']::varchar as group_name,
            data_by_row['sets']::array as group_sets,
            data_by_row['pm']::varchar as group_project_manager,
            data_by_row['pdm']::array as group_pdm,
            data_by_row['ux']::array as group_ux,
            data_by_row['uxr']::array as group_uxr,
            data_by_row['tech_writer']::varchar as group_tech_writer,
            data_by_row['tw_backup']::varchar as group_tech_writer_backup,
            data_by_row['appsec_engineer']::varchar as group_appsec_engineer
        from groups_exploded

    )

select *
from groups_parsed_out
