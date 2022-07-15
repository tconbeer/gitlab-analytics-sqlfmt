with
    source as (select * from {{ source("sheetload", "abuse_mitigation") }}),
    final as (

        select
            nullif(email_domain, '')::varchar as email_domain,
            nullif(account_creation_date, '')::varchar::date as account_creation_date,
            nullif(account_creation_time, '')::varchar::time as account_creation_time,
            nullif(account_creation_timestamp, '')::varchar::timestamp
            as account_creation_timestamp,
            nullif(category, '')::varchar as category,
            nullif(description, '')::varchar as description,
            nullif(automation, '')::varchar as automation,
            try_to_number(mitigation_week) as mitigation_week,
            try_to_number(mitigation_month) as mitigation_month,
            nullif(mitigation_date, '')::varchar::date as mitigation_date,
            nullif(mitigation_time, '')::varchar::time as mitigation_time,
            nullif(mitigation_timestamp, '')::varchar::timestamp
            as mitigation_timestamp,
            nullif(timezone, '')::varchar as timezone,
            try_to_decimal(time_to_mitigate) as time_to_mitigate,
            try_to_number(rule_id) as rule_id,
            nullif(rule_name, '')::varchar as rule_name
        from source

    )

select *
from final
