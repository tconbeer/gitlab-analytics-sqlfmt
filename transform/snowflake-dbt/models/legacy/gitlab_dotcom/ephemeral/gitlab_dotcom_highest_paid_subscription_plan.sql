{{ config({"materialized": "ephemeral"}) }}


with
    memberships as (

        select
            *,
            decode(
                membership_source_type,
                'individual_namespace',
                0,
                'group_membership',
                1,
                'project_membership',
                2,
                'group_group_link',
                3,
                'group_group_link_ancestor',
                4,
                'project_group_link',
                5,
                'project_group_link_ancestor',
                6
            ) as membership_source_type_order,
            iff(namespace_id = ultimate_parent_id, true, false) as is_ultimate_parent
        from {{ ref("gitlab_dotcom_memberships") }}
        where ultimate_parent_plan_id != 34

    ),
    plans as (select * from {{ ref("gitlab_dotcom_plans") }}),
    highest_paid_subscription_plan as (

        select distinct

            user_id,

            coalesce(
                max(plans.plan_is_paid) OVER (partition by user_id),
                false
            ) as highest_paid_subscription_plan_is_paid,

            coalesce(
                first_value(ultimate_parent_plan_id) OVER (
                    partition by user_id
                    order by
                        ultimate_parent_plan_id desc,
                        membership_source_type_order,
                        is_ultimate_parent desc,
                        membership_source_type
                ),
                34
            ) as highest_paid_subscription_plan_id,

            first_value(namespace_id) OVER (
                partition by user_id
                order by
                    ultimate_parent_plan_id desc,
                    membership_source_type_order,
                    is_ultimate_parent desc,
                    membership_source_type
            ) as highest_paid_subscription_namespace_id,

            first_value(ultimate_parent_id) OVER (
                partition by user_id
                order by
                    ultimate_parent_plan_id desc,
                    membership_source_type_order,
                    is_ultimate_parent desc,
                    membership_source_type
            ) as highest_paid_subscription_ultimate_parent_id,

            first_value(membership_source_type) OVER (
                partition by user_id
                order by
                    ultimate_parent_plan_id desc,
                    membership_source_type_order,
                    is_ultimate_parent desc,
                    membership_source_type
            ) as highest_paid_subscription_inheritance_source_type,

            first_value(membership_source_id) OVER (
                partition by user_id
                order by
                    ultimate_parent_plan_id desc,
                    membership_source_type_order,
                    is_ultimate_parent desc,
                    membership_source_type
            ) as highest_paid_subscription_inheritance_source_id

        from memberships
        left join plans on memberships.ultimate_parent_plan_id = plans.plan_id

    )

select *
from highest_paid_subscription_plan
