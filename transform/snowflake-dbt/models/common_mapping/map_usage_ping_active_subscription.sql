{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("map_subscription_lineage", "map_subscription_lineage"),
            ("prep_subscription", "prep_subscription"),
            ("fct_usage_ping_payload", "fct_usage_ping_payload"),
        ]
    )
}}

,
active_subscriptions as (

    select
        prep_subscription.*,
        strtok_to_array(subscription_lineage, ',') as subscription_lineage_array,
        array_slice(
            subscription_lineage_array, -2, -1
        )::varchar as latest_subscription_in_lineage
    from prep_subscription
    left join
        map_subscription_lineage
        on prep_subscription.dim_subscription_id
        = map_subscription_lineage.dim_subscription_id
    where
        subscription_status in (
            'Active', 'Cancelled'
        ) and subscription_start_date < subscription_end_date

),
usage_ping_with_license as (

    select
        dim_usage_ping_id, fct_usage_ping_payload.ping_created_at, dim_subscription_id
    from fct_usage_ping_payload

),
map_to_all_subscriptions_in_lineage as (

    select *, f.value as subscription_in_lineage, f.index as lineage_index
    from active_subscriptions, lateral flatten(input => subscription_lineage_array) f

),
join_ping_to_subscriptions as (
    -- this CTE is finding for a specific usage ping the current subscription_id that
    -- are valid
    -- and in the lineage of the subscription that was linked to the usage ping at
    -- creation date
    select
        dim_usage_ping_id,
        usage_ping_with_license.ping_created_at as usage_ping_created_at,
        renewal_subscriptions.subscription_start_date as subscription_start_date,
        renewal_subscriptions.subscription_end_date as subscription_end_date,
        renewal_subscriptions.subscription_name_slugify as subscription_name_slugify,
        renewal_subscriptions.dim_subscription_id as dim_subscription_id
    from usage_ping_with_license
    inner join
        prep_subscription
        on usage_ping_with_license.dim_subscription_id
        = prep_subscription.dim_subscription_id
    inner join
        map_to_all_subscriptions_in_lineage as active_subscriptions
        on active_subscriptions.subscription_name_slugify
        = prep_subscription.subscription_name_slugify
    left join
        active_subscriptions as renewal_subscriptions
        on active_subscriptions.subscription_in_lineage
        = renewal_subscriptions.subscription_name_slugify

),
first_subscription as (

    -- decision taken because there is in very little cases 1% of the cases, several
    -- active subscriptions in a lineage at on a specific month M
    select distinct
        dim_usage_ping_id,
        first_value(dim_subscription_id) over (
            partition by dim_usage_ping_id order by subscription_start_date asc
        ) as dim_subscription_id
    from join_ping_to_subscriptions
    where
        usage_ping_created_at >= subscription_start_date
        and usage_ping_created_at <= subscription_end_date

),
unioned as (

    -- FIRST CTE: valid subscriptions when the usage ping got created
    select
        join_ping_to_subscriptions.dim_usage_ping_id,
        first_subscription.dim_subscription_id,
        array_agg(join_ping_to_subscriptions.dim_subscription_id) within group(
            order by subscription_start_date asc
        ) as other_dim_subscription_id_array,
        'Match between Usage Ping and Active Subscription' as match_type
    from join_ping_to_subscriptions
    left join
        first_subscription
        on join_ping_to_subscriptions.dim_usage_ping_id
        = first_subscription.dim_usage_ping_id
    where
        usage_ping_created_at >= subscription_start_date
        and usage_ping_created_at <= subscription_end_date
    group by 1, 2

    UNION

    -- SECOND CTE: No valid subscriptions at usage ping creation
    select distinct
        join_ping_to_subscriptions.dim_usage_ping_id,
        first_value(join_ping_to_subscriptions.dim_subscription_id) over (
            partition by join_ping_to_subscriptions.dim_usage_ping_id
            order by subscription_start_date asc
        ) as dim_subscription_id,
        null as other_dim_subscription_id_array,
        'Match between Usage Ping and a expired Subscription' as match_type
    from join_ping_to_subscriptions
    left join
        first_subscription
        on join_ping_to_subscriptions.dim_usage_ping_id
        = first_subscription.dim_usage_ping_id
    where
        first_subscription.dim_usage_ping_id is null
        and join_ping_to_subscriptions.dim_subscription_id is not null

)

{{
    dbt_audit(
        cte_ref="unioned",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-06-21",
        updated_date="2021-06-21",
    )
}}
