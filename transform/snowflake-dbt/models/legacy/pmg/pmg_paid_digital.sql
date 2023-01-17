with reporting_data as (select * from {{ ref("paid_digital") }})

select

    reporting_date as reporting_date,
    medium as medium,
    source as source,
    campaign as campaign,
    campaign_code as campaign_code,
    upper(region) as region,
    targeting as targeting,
    ad_unit as ad_unit,
    nullif(brand_not_brand, 'x') as brand_not_brand,
    nullif(match_unit, 'x') as match_unit,
    content as content,
    team as team,
    budget as budget,
    sales_segment as sales_segment,
    data_source as data_source,
    impressions as impressions,
    clicks as clicks,
    conversion as conversion,
    cost as cost,
    sends as sends,
    opens as opens,
    inquiries as inquiries,
    mqls as mqls,
    linear_sao as linear_sao,
    ga_conversion as ga_conversion,
    compaign_code_type as compaign_code_type,
    content_type as content_type,
    uploaded_at as uploaded_at

from reporting_data
order by reporting_date
