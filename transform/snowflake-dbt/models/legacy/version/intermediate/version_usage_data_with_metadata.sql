{{ config({"materialized": "incremental", "unique_key": "id"}) }}


{{
    simple_cte(
        [
            ("licenses", "customers_db_licenses_source"),
            ("zuora_subscriptions", "zuora_subscription"),
            ("zuora_accounts", "zuora_account"),
            ("version_releases", "version_releases"),
        ]
    )
}},
usage_data as (

    select
        {{
            dbt_utils.star(
                from=ref("version_usage_data"),
                except=["LICENSE_STARTS_AT", "LICENSE_EXPIRES_AT"],
            )
        }}
    from {{ ref("version_usage_data") }}

),
joined as (

    select
        usage_data.*,
        licenses.license_id,
        licenses.zuora_subscription_id,
        licenses.company,
        licenses.plan_code as license_plan_code,
        licenses.license_start_date as license_starts_at,
        licenses.license_expire_date as license_expires_at,
        zuora_subscriptions.subscription_status as zuora_subscription_status,
        zuora_accounts.crm_id as zuora_crm_id,
        datediff(
            'days', ping_version.release_date, usage_data.created_at
        ) as days_after_version_release_date,
        latest_version.major_minor_version as latest_version_available_at_ping_creation,
        latest_version.version_row_number
        - ping_version.version_row_number as versions_behind_latest

    from usage_data
    left join licenses on usage_data.license_md5 = licenses.license_md5
    left join
        zuora_subscriptions
        on licenses.zuora_subscription_id = zuora_subscriptions.subscription_id
    left join
        zuora_accounts on zuora_subscriptions.account_id = zuora_accounts.account_id
    left join
        version_releases as ping_version  -- Join on the version of the ping itself.
        on usage_data.major_minor_version = ping_version.major_minor_version
    left join
        -- Join the latest version released at the time of the ping.
        version_releases as latest_version
        on usage_data.created_at
        between latest_version.release_date
        and {{ coalesce_to_infinity("latest_version.next_version_release_date") }}
    where
        (
            licenses.email is null
            -- Exclude internal tests licenses.
            or not (email like '%@gitlab.com' and lower(company) like '%gitlab%')
            or uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
        )

),
renamed as (

    select
        joined.*,
        case
            when uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
            then 'SaaS'
            else 'Self-Managed'
        end as ping_source,
        case when lower(edition) like '%ee%' then 'EE' else 'CE' end as main_edition,
        case
            when edition like '%CE%'
            then 'Core'
            when edition like '%EES%'
            then 'Starter'
            when edition like '%EEP%'
            then 'Premium'
            when edition like '%EEU%'
            then 'Ultimate'
            when edition like '%EE Free%'
            then 'Core'
            when edition like '%EE%'
            then 'Starter'
            else null
        end as edition_type,
        usage_activity_by_stage_monthly['manage'][
            'events'
        ] as monthly_active_users_last_28_days

    from joined

)

select *
from renamed
