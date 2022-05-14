{{ config({"schema": "legacy"}) }}

with
    usage_ping as (select * from {{ ref("version_usage_data_source") }}),
    instances as (

        select
            uuid as instance_id,
            min(recorded_at) as recorded_first_usage_ping_time_stamp,
            max(recorded_at) as recorded_most_recent_usage_ping_time_stamp,
            min(instance_user_count) as recorded_minimum_instance_user_count,
            max(instance_user_count) as recorded_maximum_instance_user_count,
            count(distinct version) as recorded_total_version_count,
            count(distinct edition) as recorded_total_edition_count,
            count(distinct hostname) as recorded_total_hostname_count,
            count(distinct host_id) as recorded_total_host_id_count,
            count(distinct installation_type) as recorded_total_installation_type_count

        from usage_ping
        group by uuid

    ),
    renamed as (select * from instances)


    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@kathleentam",
            updated_by="@mpeychet",
            created_date="2020-10-11",
            updated_date="2020-11-24",
        )
    }}
