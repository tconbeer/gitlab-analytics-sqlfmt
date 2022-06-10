with
    source as (select * from {{ source("snapshots", "sfdc_user_snapshots") }}),
    renamed as (

        select

            -- ids
            id as user_id,
            name as name,
            email as user_email,
            employeenumber as employee_number,

            -- info
            title as title,
            team__c as team,
            department as department,
            managerid as manager_id,
            manager_name__c as manager_name,
            isactive as is_active,
            userroleid as user_role_id,
            user_role_type__c as user_role_type,
            start_date__c as start_date,
            {{ sales_hierarchy_sales_segment_cleaning("user_segment__c") }}
            as user_segment,
            user_geo__c as user_geo,
            user_region__c as user_region,
            user_area__c as user_area,
            user_segment_geo_region_area__c as user_segment_geo_region_area,
            case
                when user_segment in ('Large', 'PubSec') then 'Large' else user_segment
            end as user_segment_grouped,
            {{ sales_segment_region_grouped("user_segment", "user_geo", "user_region") }}
            as user_segment_region_grouped,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            lastmodifiedbyid as last_modified_id,
            lastmodifieddate as last_modified_date,
            systemmodstamp,

            -- dbt last run
            convert_timezone(
                'America/Los_Angeles', convert_timezone('UTC', current_timestamp())
            ) as _last_dbt_run,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed
