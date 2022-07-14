{{ config({"materialized": "incremental", "unique_key": "snapshot_day_namespace_id"}) }}

with
    date_details as (

        select *
        from {{ ref("date_details") }}
        where
            date_actual <= current_date
            {% if is_incremental() %}
            and date_actual >= (select max(snapshot_day) from {{ this }})
            {% endif %}

    ),
    namespace_snapshots as (

        select *, ifnull(valid_to, current_timestamp) as valid_to_
        from {{ ref("gitlab_dotcom_namespaces_snapshots_base") }}
        {% if is_incremental() %}
        where
            (select max(snapshot_day) from {{ this }}) between valid_from and valid_to_
        {% endif %}

    ),
    namespace_snapshots_daily as (

        select
            {{ dbt_utils.surrogate_key(["date_actual", "namespace_id"]) }}
            as snapshot_day_namespace_id,
            date_details.date_actual as snapshot_day,
            namespace_snapshots.namespace_id,
            namespace_snapshots.parent_id,
            namespace_snapshots.owner_id,
            namespace_snapshots.namespace_type,
            namespace_snapshots.visibility_level,
            namespace_snapshots.shared_runners_minutes_limit,
            namespace_snapshots.extra_shared_runners_minutes_limit,
            namespace_snapshots.repository_size_limit,
            namespace_snapshots.namespace_created_at
        from namespace_snapshots
        inner join
            date_details
            on date_details.date_actual
            between namespace_snapshots.valid_from
            ::date and namespace_snapshots.valid_to_::date
        qualify
            row_number() over (
                partition by snapshot_day, namespace_id order by valid_to_ desc
            )
            = 1

    )

select *
from namespace_snapshots_daily
