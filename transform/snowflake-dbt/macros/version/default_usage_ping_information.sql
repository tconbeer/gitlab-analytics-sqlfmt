{% macro default_usage_ping_information() %}

-- usage ping meta data 
dim_usage_ping_id,
ping_created_at,
ping_created_at_28_days_earlier,
ping_created_at_year,
ping_created_at_month,
ping_created_at_week,
ping_created_at_date,

-- instance settings 
raw_usage_data_payload['uuid']::varchar as uuid,
ping_source,
raw_usage_data_payload['version']::varchar as instance_version,
cleaned_version,
version_is_prerelease,
major_version,
minor_version,
major_minor_version,
product_tier,
main_edition_product_tier,
raw_usage_data_payload['hostname']::varchar as hostname,
raw_usage_data_payload['host_id']::number(38, 0) as host_id,
raw_usage_data_payload['installation_type']::varchar as installation_type,
is_internal,
is_staging,

-- instance user statistics 
raw_usage_data_payload['license_billable_users']::number(
    38, 0
) as license_billable_users,
raw_usage_data_payload['active_user_count']::number(38, 0) as instance_user_count,
raw_usage_data_payload['historical_max_users']::number(38, 0) as historical_max_users,
raw_usage_data_payload['license_md5']::varchar as license_md5,


{%- endmacro -%}
