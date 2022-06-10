{{ config({"materialized": "view"}) }}

with
    category_handbook_engineering_merge_requests as (

        select * from {{ ref("category_handbook_engineering_merge_requests") }}

    ),
    handbook_engineering_merge_request_path_count_department as (

        select
            -- Foreign Keys 
            merge_request_iid,

            -- Logical Information
            merge_request_path,
            merge_request_state,
            case
                when lower(merge_request_path) like '%/handbook/engineering/%'
                then 1
                when lower(merge_request_path) like '%/handbook/support/%'
                then 1
                else 0
            end as path_count_engineering,

            -- Engineering departments 
            case
                when
                    lower(
                        merge_request_path
                    ) like '%/handbook/engineering/development/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/development_department.yml%'
                then 1
                else 0
            end as path_count_development,
            case
                when
                    lower(
                        merge_request_path
                    ) like '%/handbook/engineering/infrastructure/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/infrastructure_department.yml%'
                then 1
                else 0
            end as path_count_infrastructure,
            case
                when lower(merge_request_path) like '%/handbook/engineering/quality/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/quality_department.yml%'
                then 1
                else 0
            end as path_count_quality,
            case
                when lower(merge_request_path) like '%/handbook/engineering/security/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/security_department.yml%'
                then 1
                else 0
            end as path_count_security,
            case
                when lower(merge_request_path) like '%/handbook/support/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/customer_support_department.yml%'
                then 1
                else 0
            end as path_count_support,
            case
                when lower(merge_request_path) like '%/handbook/engineering/ux/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/ux_department.yml%'
                then 1
                else 0
            end as path_count_ux,
            case
                when
                    lower(merge_request_path) like '%/handbook/engineering/incubation/%'
                then 1
                when
                    lower(
                        merge_request_path
                    ) like '%data/performance_indicators/incubation_engineering_department.yml%'
                then 1
                else 0
            end as path_count_incubation,
            -- Metadata 
            merge_request_created_at,
            merge_request_last_edited_at,
            merge_request_merged_at,
            merge_request_updated_at

        from category_handbook_engineering_merge_requests

    )

select *
from handbook_engineering_merge_request_path_count_department
