with
    source as (select * from {{ source("sheetload", "days_sales_outstanding") }}),
    renamed as (

        select
            period::date as period,
            regexp_replace(
                total_touch_billings, '[,]', ''
            )::number as total_touch_billings,
            regexp_replace(total_beginning_ar, '[,]', '')::number as total_beginning_ar,
            regexp_replace(
                total_ar_at_end_of_period, '[,]', ''
            )::number as total_ar_at_end_of_period,
            nbr_of_days_in_period::number as nbr_of_days_in_period,
            dso::number as dso,
            regexp_replace(
                collections_based_on_formula, '[,]', ''
            )::number as collections_based_on_formula,
            regexp_replace(
                total_current_ar_close, '[,]', ''
            )::number as total_current_ar_close,
            regexp_replace(p_d_ar, '[,]', '')::number as p_d_ar,
            collection_effectiveness_index::number as collection_effectiveness_index,
            dso_trend::number as dso_trend,
            cei_trend::number as cei_trend
        from source

    )

select *
from renamed
