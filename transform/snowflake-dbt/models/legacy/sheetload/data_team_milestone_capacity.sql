with
    data_team_capacity as (select * from {{ ref("sheetload_data_team_capacity") }}),
    data_team_milestone_information as (

        select distinct
            namespace_id,
            milestone_id,
            milestone_title,
            milestone_status,
            start_date,
            due_date
        from {{ ref("gitlab_dotcom_milestones_xf") }}
        where namespace_id = '4347861' and start_date > '2020-06-30'

    ),
    final as (

        select
            data_team_milestone_information.milestone_title as milestone_title,
            data_team_milestone_information.start_date as milestone_start_date,
            data_team_milestone_information.due_date as milestone_due_date,
            data_team_milestone_information.milestone_status as milestone_status,
            data_team_capacity.gitlab_handle as data_team_member_gitlab_handle,
            data_team_capacity.capacity as data_team_member_capacity
        from data_team_milestone_information
        left join
            data_team_capacity
            on data_team_milestone_information.milestone_id
            = data_team_capacity.milestone_id
    )

select *
from final
