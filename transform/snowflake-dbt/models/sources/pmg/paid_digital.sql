with
    source as (select * from {{ source("pmg", "paid_digital") }}),
    intermediate as (

        select d.value as data_by_row, uploaded_at
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    parsed as (
        select
            data_by_row['date']::timestamp as reporting_date,
            data_by_row['utm_medium']::varchar as medium,
            data_by_row['utm_source']::varchar as source,
            data_by_row['utm_campaign']::varchar as campaign,
            data_by_row['campaign_code']::varchar as campaign_code,
            data_by_row['geo']::varchar as region,
            data_by_row['targeting']::varchar as targeting,
            data_by_row['ad_unit']::varchar as ad_unit,
            data_by_row['br_nb']::varchar as brand_not_brand,
            data_by_row['match_unit']::varchar as match_unit,
            data_by_row['content']::varchar as content,
            data_by_row['team']::varchar as team,
            data_by_row['budget']::varchar as budget,
            data_by_row['sales_segment']::varchar as sales_segment,
            data_by_row['data_source']::varchar as data_source,
            data_by_row['impressions']::number as impressions,
            data_by_row['clicks']::number as clicks,
            data_by_row['conversions']::number as conversion,
            data_by_row['cost']::float as cost,
            data_by_row['sends']::varchar as sends,
            data_by_row['opens']::varchar as opens,
            data_by_row['inquiries']::varchar as inquiries,
            data_by_row['mqls']::varchar as mqls,
            data_by_row['linear_sao']::varchar as linear_sao,
            data_by_row['ga_conversions']::varchar as ga_conversion,
            data_by_row['campaign_code_type']::varchar as compaign_code_type,
            data_by_row['content_type']::varchar as content_type,
            uploaded_at::timestamp as uploaded_at
        from intermediate
    )
select *
from parsed
