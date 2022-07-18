with
    sfdc_ps_engagement as (

        select * from {{ ref("sfdc_professional_services_engagement") }}

    ),
    sfdc_users as (select * from {{ ref("sfdc_users_xf") }}),
    joined as (

        select
            sfdc_ps_engagement.*,
            sfdc_users.name as ps_engagement_owner,
            sfdc_users.manager_name as ps_engagement_owner_manager,
            sfdc_users.department as ps_engagement_owner_department,
            sfdc_users.title as ps_engagement_owner_title
        from sfdc_ps_engagement
        left join sfdc_users on sfdc_ps_engagement.owner_id = sfdc_users.user_id

    )

select *
from joined
