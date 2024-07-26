{{ config({"schema": "legacy"}) }}

with
    sfdc_executive_business_review as (

        select * from {{ ref("sfdc_executive_business_review") }}

    ),
    sfdc_users as (select * from {{ ref("sfdc_users_xf") }}),
    joined as (

        select
            sfdc_executive_business_review.*,
            sfdc_users.name as ebr_owner,
            sfdc_users.manager_name as ebr_owner_manager,
            sfdc_users.department as ebr_owner_department,
            sfdc_users.title as ebr_owner_title
        from sfdc_executive_business_review
        left join
            sfdc_users on sfdc_users.user_id = sfdc_executive_business_review.owner_id

    )

select *
from joined
