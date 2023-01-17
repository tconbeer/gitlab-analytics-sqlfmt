with
    source as (select * from {{ source("zoominfo", "global") }}),

    renamed as (

        select
            zi_c_location_id::number as location_id,
            zi_c_company_id::number as company_id,
            zi_c_is_hq::boolean as is_headquarters,
            zi_es_ecid::number as everstring_company_id,
            zi_c_latitude::float as company_latitude,
            zi_c_longitude::float as company_longitude,
            zi_c_verified_address::boolean as has_company_verified_address,
            zi_c_employees::number as employees,
            zi_c_estimated_age::number as estimated_age,
            zi_c_is_b2b::boolean as is_b2b,
            zi_c_is_b2c::boolean as is_b2c,
            zi_es_hq_ecid::number as everstring_headquarters_id,
            zi_c_company_latitude::float as headquarters_company_latitude,
            zi_c_company_longitude::float as headquarters_company_longitude,
            zi_c_company_employees::number as headquarters_employees,
            zi_c_alexa_rank::number as alexa_rank,
            zi_c_num_keywords::number as number_of_keywords,
            zi_c_employee_growth_1yr::float as employee_growth_1yr,
            zi_c_employee_growth_2yr::float as employee_growth_2yr,
            zi_es_percent_employee_growth::float as everstring_percent_employee_growth,
            zi_es_percent_revenue_growth::float as everstring_percent_revenue_growth,
            zi_c_name_confidence_score::float as company_name_confidence_score,
            zi_c_url_confidence_score::float as company_url_confidence_score,
            zi_c_address_confidence_score::float as company_address_confidence_score,
            zi_c_phone_confidence_score::float as company_phone_confidence_score,
            zi_c_employees_confidence_score::float as employees_confidence_score,
            zi_c_revenue_confidence_score::float as revenue_confidence_score,
            zi_es_industry_confidence_score::float
            as everstring_industry_confidence_score,
            zi_c_naics_confidence_score::float as naics_confidence_score,
            zi_c_sic_confidence_score::float as sic_confidence_score,
            zi_c_url_last_updated::date as url_last_updated,
            zi_c_inactive_flag::boolean as is_inactive,
            zi_c_is_small_business::boolean as is_small_business,
            zi_c_is_public::boolean as is_public,
            zi_c_has_mobile_app::boolean as has_mobile_app,
            zi_c_num_locations::number as number_of_locations,
            zi_c_hr_contacts::number as number_of_hr_contacts,
            zi_c_sales_contacts::number as number_of_sales_contacts,
            zi_c_marketing_contacts::number as number_of_marketing_contacts,
            zi_c_finance_contacts::number as number_of_finance_contacts,
            zi_c_c_suite_contacts::number as number_of_c_suite_contacts,
            zi_c_engineering_contacts::number as number_of_engineering_contacts,
            zi_c_it_contacts::number as number_of_it_contacts,
            zi_c_operations_contacts::number as number_of_operations_contacts,
            zi_c_legal_contacts::number as number_of_legal_contacts,
            zi_c_medical_contacts::number as number_of_medical_contacts,
            zi_c_latest_funding_age::number as latest_funding_age,
            zi_c_num_of_investors::number as number_of_investors,
            zi_c_total_funding_amount::number as total_funding_amount,
            zi_c_latest_funding_amount::number as latest_funding_amount,
            zi_c_num_funding_rounds::number as number_of_funding_rounds,
            zi_c_is_fortune_100::boolean as is_fortune_100,
            zi_c_is_fortune_500::boolean as is_fortune_500,
            zi_c_is_s_and_p_500::boolean as is_s_and_p_500,
            zi_c_is_domestic_hq::boolean as is_domestic_hq,
            zi_c_is_global_parent::boolean as is_global_parent,
            zi_c_is_subsidiary::boolean as is_subsidiary,
            zi_c_is_franchisor::boolean as is_franchisor,
            zi_c_is_franchisee::boolean as is_franchisee,
            zi_c_hierarchy_level::number as hierarchy_level,
            zi_c_parent_child_confidence_score::float as parent_child_confidence_score,
            zi_c_immediate_parent_company_id::number as immediate_parent_company_id,
            zi_es_immediate_parent_ecid::number as everstring_immediate_parent_id,
            zi_c_domestic_parent_company_id::number as domestic_parent_company_id,
            zi_es_domestic_parent_ecid::number as everstring_domestic_parent_id,
            zi_c_global_parent_company_id::number as global_parent_company_id,
            zi_es_global_parent_ecid::number as everstring_global_parent_id,
            zi_c_franchisor_company_id::number as franchisor_company_id,
            zi_es_franchisor_ecid::number as everstring_franchisor_id,
            zi_c_last_updated_date::date as last_updated_date,
            nullif(zi_es_location_id::varchar, '') as everstring_location_id,
            nullif(zi_c_tier_grade::varchar, '') as tier_grade,
            nullif(zi_c_name::varchar, '') as location_name,
            nullif(zi_c_name_display::varchar, '') as colloquial_name,
            nullif(zi_c_legal_entity_type::varchar, '') as legal_entity_type,
            nullif(zi_c_url::varchar, '') as company_url,
            nullif(zi_c_street::varchar, '') as company_street,
            nullif(zi_c_street_2::varchar, '') as company_street_2,
            nullif(zi_c_city::varchar, '') as company_city,
            nullif(zi_c_state::varchar, '') as company_state,
            nullif(zi_c_zip::varchar, '') as company_zip,
            nullif(zi_c_country::varchar, '') as company_country,
            nullif(zi_c_cbsa_name::varchar, '') as cbsa_name,
            nullif(zi_c_county::varchar, '') as company_county,
            nullif(zi_c_employee_range::varchar, '') as employee_range,
            nullif(zi_c_revenue_range::varchar, '') as revenue_range,
            zi_c_revenue::number * 1000 as revenue,  -- convert from thousand
            nullif(zi_c_phone::varchar, '') as company_phone,
            nullif(zi_c_fax::varchar, '') as company_fax,
            nullif(zi_c_industry_primary::varchar, '') as industry_primary,
            nullif(zi_c_sub_industry_primary::varchar, '') as sub_industry_primary,
            nullif(zi_c_industries::varchar, '') as industries,
            nullif(zi_c_sub_industries::varchar, '') as sub_industries,
            nullif(zi_es_industry::varchar, '') as everstring_industry,
            nullif(zi_es_industries_top3::varchar, '') as everstring_industries_top3,
            nullif(zi_c_naics2::varchar, '') as naics_2_code,
            nullif(zi_c_naics4::varchar, '') as naics_4_code,
            nullif(zi_c_naics6::varchar, '') as naics_6_code,
            nullif(zi_c_naics_top3::varchar, '') as naics_top3,
            nullif(zi_c_sic2::varchar, '') as sic_2_code,
            nullif(zi_c_sic3::varchar, '') as sic_3_code,
            nullif(zi_c_sic4::varchar, '') as sic_4_code,
            nullif(zi_c_sic_top3::varchar, '') as sic_top3,
            nullif(zi_c_year_founded::varchar, '') as year_founded,
            nullif(
                zi_es_hq_location_id::varchar, ''
            ) as everstring_headquarters_location_id,
            nullif(zi_c_company_name::varchar, '') as headquarters_company_name,
            nullif(zi_c_company_url::varchar, '') as headquarters_company_url,
            nullif(zi_c_company_street::varchar, '') as headquarters_company_street,
            nullif(zi_c_company_street_2::varchar, '') as headquarters_company_street_2,
            nullif(zi_c_company_city::varchar, '') as headquarters_company_city,
            nullif(zi_c_company_state::varchar, '') as headquarters_company_state,
            nullif(zi_c_company_zip::varchar, '') as headquarters_company_zip,
            nullif(zi_c_company_country::varchar, '') as headquarters_company_country,
            nullif(
                zi_c_company_cbsa_name::varchar, ''
            ) as headquarters_company_cbsa_name,
            nullif(zi_c_company_county::varchar, '') as headquarters_company_county,
            nullif(zi_c_company_verified_address, '')::boolean
            as has_headquarters_company_verified_address,
            nullif(
                zi_c_company_employee_range::varchar, ''
            ) as headquarters_employee_range,
            nullif(
                zi_c_company_revenue_range::varchar, ''
            ) as headquarters_revenue_range,
            -- convert from thousand
            zi_c_company_revenue::number * 1000 as headquarters_revenue,
            nullif(zi_c_company_phone::varchar, '') as headquarters_company_phone,
            nullif(zi_c_company_fax::varchar, '') as headquarters_company_fax,
            nullif(zi_c_linkedin_url::varchar, '') as linkedin_url,
            nullif(zi_c_facebook_url::varchar, '') as facebook_url,
            nullif(zi_c_twitter_url::varchar, '') as twitter_url,
            nullif(zi_c_yelp_url::varchar, '') as yelp_url,
            nullif(zi_c_keywords::varchar, '') as company_keywords,
            nullif(zi_c_top_keywords::varchar, '') as top_keywords,
            nullif(zi_es_growth::varchar, '') as everstring_growth,
            nullif(zi_es_employee_growth::varchar, '') as everstring_employee_growth,
            nullif(zi_es_revenue_growth::varchar, '') as everstring_revenue_growth,
            nullif(
                zi_es_industries_top3_confidence_scores::varchar, ''
            ) as everstring_industries_top3_confidence_scores,
            nullif(
                zi_c_naics_top3_confidence_scores::varchar, ''
            ) as naics_top3_confidence_scores,
            nullif(
                zi_c_sic_top3_confidence_scores::varchar, ''
            ) as sic_top3_confidence_scores,
            nullif(zi_c_ids_merged::varchar, '') as merged_previous_company_ids,
            nullif(zi_c_names_other::varchar, '') as other_company_names,
            nullif(zi_c_url_status::varchar, '') as company_url_status,
            nullif(zi_c_urls_alt::varchar, '') as alternate_company_urls,
            nullif(zi_c_ein::varchar, '') as company_ein,
            nullif(zi_c_ticker::varchar, '') as company_ticker,
            nullif(zi_c_tickers_alt::varchar, '') as alternate_company_tickers,
            nullif(zi_c_currency_code::varchar, '') as currency_code,
            nullif(zi_c_tech_ids::varchar, '') as tech_ids,
            nullif(zi_c_investor_names::varchar, '') as investor_names,
            nullif(zi_c_funding_strength::varchar, '') as funding_strength,
            nullif(zi_c_funding_type::varchar, '') as funding_type,
            nullif(zi_c_latest_funding_date, '')::date as latest_funding_date,
            nullif(zi_c_hierarchy_code::varchar, '') as hierarchy_code,
            nullif(
                zi_es_immediate_parent_location_id::varchar, ''
            ) as everstring_immediate_parent_location_id,
            nullif(zi_c_immediate_parent_name::varchar, '') as immediate_parent_name,
            nullif(zi_c_immediate_parent_url::varchar, '') as immediate_parent_url,
            nullif(
                zi_c_immediate_parent_street::varchar, ''
            ) as immediate_parent_street,
            nullif(
                zi_c_immediate_parent_street_2::varchar, ''
            ) as immediate_parent_street_2,
            nullif(zi_c_immediate_parent_city::varchar, '') as immediate_parent_city,
            nullif(zi_c_immediate_parent_zip::varchar, '') as immediate_parent_zip,
            nullif(zi_c_immediate_parent_state::varchar, '') as immediate_parent_state,
            nullif(
                zi_c_immediate_parent_country::varchar, ''
            ) as immediate_parent_country,
            nullif(
                zi_es_domestic_parent_location_id::varchar, ''
            ) as everstring_domestic_parent_location_id,
            nullif(zi_c_domestic_parent_name::varchar, '') as domestic_parent_name,
            nullif(zi_c_domestic_parent_url::varchar, '') as domestic_parent_url,
            nullif(zi_c_domestic_parent_street::varchar, '') as domestic_parent_street,
            nullif(
                zi_c_domestic_parent_street_2::varchar, ''
            ) as domestic_parent_street_2,
            nullif(zi_c_domestic_parent_city::varchar, '') as domestic_parent_city,
            nullif(zi_c_domestic_parent_zip::varchar, '') as domestic_parent_zip,
            nullif(zi_c_domestic_parent_state::varchar, '') as domestic_parent_state,
            nullif(
                zi_c_domestic_parent_country::varchar, ''
            ) as domestic_parent_country,
            nullif(
                zi_es_global_parent_location_id::varchar, ''
            ) as everstring_global_parent_location_id,
            nullif(zi_c_global_parent_name::varchar, '') as global_parent_name,
            nullif(zi_c_global_parent_url::varchar, '') as global_parent_url,
            nullif(zi_c_global_parent_street::varchar, '') as global_parent_street,
            nullif(zi_c_global_parent_street_2::varchar, '') as global_parent_street_2,
            nullif(zi_c_global_parent_city::varchar, '') as global_parent_city,
            nullif(zi_c_global_parent_zip::varchar, '') as global_parent_zip,
            nullif(zi_c_global_parent_state::varchar, '') as global_parent_state,
            nullif(zi_c_global_parent_country::varchar, '') as global_parent_country,
            nullif(
                zi_es_franchisor_location_id::varchar, ''
            ) as everstring_franchisor_location_id,
            nullif(zi_c_franchisor_name::varchar, '') as franchisor_name,
            nullif(zi_c_franchisor_url::varchar, '') as franchisor_url,
            nullif(zi_c_franchisor_street::varchar, '') as franchisor_street,
            nullif(zi_c_franchisor_street_2::varchar, '') as franchisor_street_2,
            nullif(zi_c_franchisor_city::varchar, '') as franchisor_city,
            nullif(zi_c_franchisor_zip::varchar, '') as franchisor_zip,
            nullif(zi_c_franchisor_state::varchar, '') as franchisor_state,
            nullif(zi_c_franchisor_country::varchar, '') as franchisor_country,
            to_date(zi_c_release_date, 'YYYYMM')::date as release_date
        from source

    )

select *
from renamed
