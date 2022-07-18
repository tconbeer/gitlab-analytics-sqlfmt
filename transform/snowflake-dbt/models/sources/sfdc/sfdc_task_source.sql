with
    source as (select * from {{ source("salesforce", "task") }}),
    renamed as (

        select
            id as task_id,

            -- keys
            accountid as account_id,
            ownerid as owner_id,
            whoid as lead_or_contact_id,
            whatid as account_or_opportunity_id,

            -- info
            description as full_comments,
            subject as task_subject,
            activitydate as task_date,
            isdeleted as is_deleted,
            status as status,
            type as type,
            createddate as task_created_date,

            assigned_employee_number__c as assigned_employee_number,
            -- Original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
            persona_functions__c as persona_functions,
            persona_levels__c as persona_levels,
            sa_activity_type__c as sa_activity_type

        from source
    )

select *
from renamed
