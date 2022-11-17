with
    mapping_table as (

        select * from {{ ref("sheetload_mapping_sdr_sfdc_bamboohr_source") }}

    ),
    sfdc_users as (

        select *, substr(user_id, 1, 15) as trim_mapping
        from {{ ref("sfdc_users_source") }}

    )

select
    sfdc_users.user_id,
    mapping_table.user_id as fifteen_length_user_id,
    mapping_table.first_name,
    mapping_table.last_name,
    mapping_table.username,
    mapping_table.active,
    mapping_table.profile,
    mapping_table.eeid,
    mapping_table.sdr_segment,
    mapping_table.sdr_region,
    mapping_table.sdr_order_type

from mapping_table
left join sfdc_users on mapping_table.user_id = sfdc_users.trim_mapping
