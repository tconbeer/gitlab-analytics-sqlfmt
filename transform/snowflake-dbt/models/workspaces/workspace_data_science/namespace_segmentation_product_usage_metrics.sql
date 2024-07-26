{{ config(materialized="table", tags=["mnpi_exception"]) }}

with
    days_of_usage as (

        select

            namespace_id,
            count(
                distinct(
                    case
                        when stage_name = 'create'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_create_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'protect'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_protect_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'package'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_package_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'plan'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_plan_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'secure'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_secure_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'verify'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_verify_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'configure'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_configure_all_time_cnt,
            count(
                distinct(
                    case
                        when stage_name = 'release'
                        then days_since_namespace_creation
                        else null
                    end
                )
            ) as days_usage_in_stage_release_all_time_cnt
        from {{ ref("gitlab_dotcom_usage_data_events") }}
        where
            stage_name not in ('monitor', 'manage') and project_is_learn_gitlab != true
        group by 1

    )

select *
from days_of_usage
