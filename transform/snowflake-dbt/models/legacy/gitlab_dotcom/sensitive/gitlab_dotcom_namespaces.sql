with
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces_source") }}),
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions") }}
        where is_currently_valid = true

    ),
    joined as (

        select namespaces.*, coalesce(gitlab_subscriptions.plan_id, 34) as plan_id
        from namespaces
        left join
            gitlab_subscriptions
            on namespaces.namespace_id = gitlab_subscriptions.namespace_id

    )

select *
from joined
