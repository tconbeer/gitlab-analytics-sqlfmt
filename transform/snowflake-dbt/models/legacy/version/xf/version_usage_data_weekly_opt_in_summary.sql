{{ config({"materialized": "table"}) }}


with
    licenses as (

        select *
        from {{ ref("customers_db_licenses_source") }}
        where
            license_md5 is not null
            and is_trial = false
            -- Remove internal test licenses
            and not (email like '%@gitlab.com' and lower(company) like '%gitlab%')

    ),
    usage_data as (

        select *
        from {{ ref("version_usage_data_unpacked") }}
        where license_md5 is not null

    ),
    week_spine as (

        select distinct date_trunc('week', date_actual) as week
        from {{ ref("date_details") }}
        where date_details.date_actual between '2017-04-01' and current_date

    ),
    grouped as (

        select
            week_spine.week,
            licenses.license_id,
            licenses.license_md5,
            licenses.zuora_subscription_id,
            licenses.plan_code as product_category,
            max(iff(usage_data.id is not null, 1, 0)) as did_send_usage_data,
            count(distinct usage_data.id) as count_usage_data_pings,
            min(usage_data.created_at) as min_usage_data_created_at,
            max(usage_data.created_at) as max_usage_data_created_at
        from week_spine
        left join licenses on week_spine.week between licenses.license_start_date and {{
                coalesce_to_infinity(
                    "licenses.license_expire_date"
                )
            }}
        left join
            usage_data
            on licenses.license_md5 = usage_data.license_md5
            and week_spine.week = date_trunc('week', usage_data.created_at)
            {{ dbt_utils.group_by(n=5) }}

    ),
    alphabetized as (

        select
            week,
            license_id,
            license_md5,
            product_category,
            zuora_subscription_id,

            -- metadata
            count_usage_data_pings,
            did_send_usage_data::boolean as did_send_usage_data,
            min_usage_data_created_at,
            max_usage_data_created_at
        from grouped

    )

select *
from alphabetized
