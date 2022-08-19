with
    sfdc_opportunity_xf as (select * from {{ ref("sfdc_opportunity_xf") }}),
    filtered as (

        select account_id, max(close_date) as close_date
        from sfdc_opportunity_xf
        where close_date < current_date and is_won = true and stage_is_closed = true
        group by 1

    ),
    xf as (

        select filtered.*, max(sfdc_opportunity_xf.incremental_acv) as incremental_acv
        from filtered
        left join
            sfdc_opportunity_xf
            on sfdc_opportunity_xf.account_id = filtered.account_id
            and sfdc_opportunity_xf.close_date::date = filtered.close_date::date
        group by 1, 2

    ),
    final as (select *, {{ sfdc_deal_size("incremental_acv", "deal_size") }} from xf)

select *
from final
