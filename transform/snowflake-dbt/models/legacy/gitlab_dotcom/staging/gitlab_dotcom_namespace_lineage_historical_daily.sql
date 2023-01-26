{{ config({"materialized": "incremental", "unique_key": "snapshot_day_namespace_id"}) }}

{{
    simple_cte(
        [
            ("map_namespace_internal", "map_namespace_internal"),
            (
                "namespace_subscription_snapshots",
                "gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base",
            ),
            ("namespace_lineage", "gitlab_dotcom_namespace_lineage_scd"),
        ]
    )
}},

dates as (
    select *
    from {{ ref("dim_date") }}  -- prod.common.dim_date
    where
        date_actual <= current_date()
        {% if is_incremental() -%}
        and date_actual >= (select max(snapshot_day) from {{ this }})
        {%- endif %}
),
namespace_lineage_daily as (
    select
        dates.date_actual as snapshot_day,
        namespace_lineage.namespace_id,
        namespace_lineage.parent_id,
        namespace_lineage.upstream_lineage,
        namespace_lineage.ultimate_parent_id
    from namespace_lineage
    inner join
        dates
        on dates.date_actual
        between date_trunc('day', namespace_lineage.lineage_valid_from) and date_trunc(
            'day', namespace_lineage.lineage_valid_to
        )
    qualify
        row_number() over (
            partition by dates.date_actual, namespace_id
            order by namespace_lineage.lineage_valid_to desc
        )
        = 1
),

with_plans as (

    select
        namespace_lineage_daily.*,
        ifnull(
            map_namespace_internal.ultimate_parent_namespace_id is not null, false
        ) as namespace_is_internal,
        iff(
            namespace_subscription_snapshots.is_trial
            and ifnull(namespace_subscription_snapshots.plan_id, 34) not in (
                34, 103
            -- Excluded Premium (103) and Free (34) Trials from being remapped as
            -- Ultimate Trials
            ),
            -- All historical trial GitLab subscriptions were Ultimate/Gold Trials
            -- (102)
            102,
            ifnull(namespace_subscription_snapshots.plan_id, 34)
        ) as ultimate_parent_plan_id,
        namespace_subscription_snapshots.seats,
        namespace_subscription_snapshots.seats_in_use,
        namespace_subscription_snapshots.max_seats_used
    from namespace_lineage_daily
    left join
        map_namespace_internal
        on namespace_lineage_daily.ultimate_parent_id
        = map_namespace_internal.ultimate_parent_namespace_id
    left join
        namespace_subscription_snapshots
        on namespace_lineage_daily.ultimate_parent_id
        = namespace_subscription_snapshots.namespace_id
        and namespace_lineage_daily.snapshot_day
        between namespace_subscription_snapshots.valid_from::date and ifnull(
            namespace_subscription_snapshots.valid_to::date, current_date
        )
    qualify
        row_number() over (
            partition by namespace_lineage_daily.namespace_id, snapshot_day
            order by valid_from desc
        )
        = 1

)

select
    {{ dbt_utils.surrogate_key(["snapshot_day", "namespace_id"]) }}
    as snapshot_day_namespace_id,
    *
from with_plans
