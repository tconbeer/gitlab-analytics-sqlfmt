{{
    simple_cte(
        [
            ("prep_gitlab_dotcom_plan", "prep_gitlab_dotcom_plan"),
            ("dim_date", "dim_date"),
        ]
    )
}},
source as (

    select *
    from
        {{
            source(
                "snapshots",
                "gitlab_dotcom_gitlab_subscriptions_namespace_id_snapshots",
            )
        }}
    where
        id
        != 572635  -- This ID has NULL values for many of the important columns.

),
renamed as (

    select
        dbt_scd_id::varchar as dim_namespace_plan_subscription_hist_id,
        id::number as dim_plan_subscription_id,
        start_date::date as plan_subscription_start_date,
        end_date::date as plan_subscription_end_date,
        trial_ends_on::date as plan_subscription_trial_end_date,
        namespace_id::number as dim_namespace_id,
        hosted_plan_id::number as dim_plan_id,
        max_seats_used::number as max_seats_used,
        seats::number as seats,
        trial::boolean as is_trial,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        seats_in_use::number as seats_in_use,
        seats_owed::number as seats_owed,
        trial_extension_type::number as trial_extension_type,
        "DBT_VALID_FROM"::timestamp as valid_from,
        "DBT_VALID_TO"::timestamp as valid_to
    from source

),
joined as (

    select
        -- primary key
        renamed.dim_namespace_plan_subscription_hist_id,

        -- foreign keys
        renamed.dim_plan_subscription_id,
        renamed.dim_namespace_id,
        -- asusming if dim_plan_id is null that it is a free plan
        ifnull(renamed.dim_plan_id, 34) as dim_plan_id,

        -- date dimensions
        plan_subscription_start_date.date_id as plan_subscription_start_date_id,
        plan_subscription_end_date.date_id as plan_subscription_end_date_id,
        plan_subscription_trial_end_date.date_id as plan_subscription_trial_end_date_id,

        -- namespace_plan metadata
        renamed.max_seats_used,
        renamed.seats,
        renamed.is_trial,
        renamed.created_at,
        renamed.updated_at,
        renamed.seats_in_use,
        renamed.seats_owed,
        renamed.trial_extension_type,

        -- hist dimensions
        renamed.valid_from,
        renamed.valid_to
    from renamed
    left join
        dim_date as plan_subscription_start_date
        on renamed.plan_subscription_start_date = plan_subscription_start_date.date_day
    left join
        dim_date as plan_subscription_end_date
        on renamed.plan_subscription_end_date = plan_subscription_end_date.date_day
    left join
        dim_date as plan_subscription_trial_end_date
        on renamed.plan_subscription_trial_end_date
        = plan_subscription_trial_end_date.date_day
    left join
        prep_gitlab_dotcom_plan
        on renamed.dim_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
    where renamed.dim_namespace_id is not null

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-05-30",
        updated_date="2021-05-30",
    )
}}
