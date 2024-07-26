{{ config(tags=["product"]) }}

{{ config({"materialized": "incremental", "unique_key": "dim_user_id"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("source", "gitlab_dotcom_users_source"),
            ("email_classification", "driveload_email_domain_classification_source"),
        ]
    )
}},
email_classification_dedup as (

    select *
    from email_classification
    qualify row_number() over (partition by domain order by domain desc) = 1

),
renamed as (

    select
        user_id as dim_user_id,
        remember_created_at as remember_created_at,
        sign_in_count as sign_in_count,
        current_sign_in_at as current_sign_in_at,
        last_sign_in_at as last_sign_in_at,
        created_at as created_at,
        dim_date.date_id as created_date_id,
        updated_at as updated_at,
        is_admin as is_admin,
        state as user_state,
        case
            when state in ('blocked', 'banned') then true else false
        end as is_blocked_user,
        source.notification_email_domain as notification_email_domain,
        notification_email_domain.classification
        as notification_email_domain_classification,
        source.email_domain as email_domain,
        email_domain.classification as email_domain_classification,
        source.public_email_domain as public_email_domain,
        public_email_domain.classification as public_email_domain_classification,
        source.commit_email_domain as commit_email_domain,
        commit_email_domain.classification as commit_email_domain_classification

    from source
    left join dim_date on to_date(source.created_at) = dim_date.date_day
    left join
        email_classification_dedup as notification_email_domain
        on notification_email_domain.domain = source.notification_email_domain
    left join
        email_classification_dedup as email_domain
        on email_domain.domain = source.email_domain
    left join
        email_classification_dedup as public_email_domain
        on public_email_domain.domain = source.public_email_domain
    left join
        email_classification_dedup as commit_email_domain
        on commit_email_domain.domain = source.commit_email_domain

)

{{
    dbt_audit(
        cte_ref="renamed",
        created_by="@mpeychet",
        updated_by="@jpeguero",
        created_date="2021-05-31",
        updated_date="2022-04-26",
    )
}}
