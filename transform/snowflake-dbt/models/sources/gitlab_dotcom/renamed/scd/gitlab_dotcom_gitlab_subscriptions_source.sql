with
    {{ distinct_source(source=source("gitlab_dotcom", "gitlab_subscriptions")) }},
    renamed as (

        select distinct

            id::number as gitlab_subscription_id,
            start_date::date as gitlab_subscription_start_date,
            end_date::date as gitlab_subscription_end_date,
            trial_starts_on::date as gitlab_subscription_trial_starts_on,
            trial_ends_on::date as gitlab_subscription_trial_ends_on,
            namespace_id::number as namespace_id,
            hosted_plan_id::number as plan_id,
            max_seats_used::number as max_seats_used,
            seats::number as seats,
            trial::boolean as is_trial,
            seats_in_use::number as seats_in_use,
            seats_owed::number as seats_owed,
            trial_extension_type::number as trial_extension_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            valid_from  -- Column was added in distinct_source CTE

        from distinct_source
        where
            gitlab_subscription_id != 572635  -- This ID has NULL values for many of the important columns. 
            and namespace_id is not null

    )

    /* Note: the primary key used is namespace_id, not subscription id.
   This matches our business use case better. */
    {{ scd_type_2(primary_key_renamed="namespace_id", primary_key_raw="namespace_id") }}
