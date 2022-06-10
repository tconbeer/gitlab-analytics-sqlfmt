with
    source as (select * from {{ source("sheetload", "rfs_support_requests") }}),
    final as (

        select
            nullif(customer_prospect_name, '')::varchar as customer_prospect_name,
            nullif(request_type, '')::varchar as request_type,
            nullif(market_industry_vertical, '')::varchar as market_industry_vertical,
            nullif(customer_prospect_size, '')::varchar as customer_prospect_size,
            nullif(sfdc_link, '')::varchar as sfdc_link,
            nullif(iacv_impact, '')::varchar as iacv_impact,
            nullif(product_host, '')::varchar as product_host,
            nullif(due_date, '')::varchar as due_date,
            nullif(other, '')::varchar as other,
            nullif(requestor_name, '')::varchar as requestor_name,
            nullif(
                additional_gitlab_team_members, ''
            )::varchar as additional_gitlab_team_members,
            nullif(month, '')::date as date
        from source

    )

select *
from final
