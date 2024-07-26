{{ config(tags=["mnpi_exception"]) }}

with
    detection_rule as (select * from {{ ref("data_detection_rule") }}),
    rule_run_detail as (

        select
            rule_id,
            processed_record_count,
            passed_record_count,
            failed_record_count,
            (
                (passed_record_count / processed_record_count) * 100
            ) as percent_of_records_passed,
            (
                (failed_record_count / processed_record_count) * 100
            ) as percent_of_records_failed,
            rule_run_date,
            type_of_data
        from {{ ref("product_data_detection_run_detail") }}

    ),
    final as (

        select distinct
            detection_rule.rule_id,
            detection_rule.rule_name,
            detection_rule.rule_description,
            rule_run_detail.rule_run_date,
            rule_run_detail.percent_of_records_passed,
            rule_run_detail.percent_of_records_failed,
            iff(percent_of_records_passed > threshold, true, false) as is_pass,
            rule_run_detail.type_of_data
        from rule_run_detail
        left outer join
            detection_rule on rule_run_detail.rule_id = detection_rule.rule_id

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@snalamaru",
            updated_by="@snalamaru",
            created_date="2021-06-16",
            updated_date="2021-06-16",
        )
    }}
