with
    source as (

        select * from {{ source("demandbase", "campaign_account_performance") }}

    ),
    renamed as (

        select
            jsontext['account_id']::number as account_id,
            jsontext['campaign_id']::number as campaign_id,
            jsontext['click_count']::number as click_count,
            jsontext['click_through_rate']::number as click_through_rate,
            jsontext['effective_cpc_cents']::number as effective_cpc_cents,
            jsontext['effective_cpm_cents']::number as effective_cpm_cents,
            jsontext['effective_spend_cents']::number as effective_spend_cents,
            jsontext['impression_count']::number as impression_count,
            jsontext['is_current_account']::boolean as is_current_account,
            jsontext['page_view_count']::number as page_view_count,
            jsontext['partition_date']::date as partition_date
        from source
        where
            partition_date = (select max(jsontext['partition_date']::date) from source)

    )

select *
from renamed
