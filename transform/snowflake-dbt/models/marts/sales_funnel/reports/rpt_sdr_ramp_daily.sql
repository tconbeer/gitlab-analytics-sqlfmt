{{
    simple_cte(
        [
            (
                "bamboohr_job_info_current_division_base",
                "bamboohr_job_info_current_division_base",
            ),
            (
                "sheetload_mapping_sdr_sfdc_bamboohr",
                "sheetload_mapping_sdr_sfdc_bamboohr",
            ),
            ("dim_crm_user", "dim_crm_user"),
            ("dim_date", "dim_date"),
        ]
    )
}},
sdr_prep as (

    select
        employee_id,
        job_role,
        min(effective_date) as start_date,
        max(ifnull(effective_end_date, '2030-12-12')) as emp_end_date,
        max(termination_date) as termination_date
    from bamboohr_job_info_current_division_base
    where
        lower(job_title) like '%sales development representative%'
        or lower(job_title) like '%sales development team lead%'
        or lower(job_title) like '%business development representative%'
        or lower(job_title) like '%sales development rep%'
    group by 1, 2

),
sdr as (

    select
        sdr_prep.*, coalesce(termination_date, emp_end_date) as company_or_role_end_date
    from sdr_prep

),
sdr_ramp as (

    select
        sdr.*,
        sheetload_mapping_sdr_sfdc_bamboohr.first_name,
        sheetload_mapping_sdr_sfdc_bamboohr.last_name,
        sheetload_mapping_sdr_sfdc_bamboohr.active,
        sheetload_mapping_sdr_sfdc_bamboohr.user_id as dim_crm_user_id,
        sheetload_mapping_sdr_sfdc_bamboohr.sdr_segment,
        sheetload_mapping_sdr_sfdc_bamboohr.sdr_region,
        iff(
            sheetload_mapping_sdr_sfdc_bamboohr.sdr_region in ('East', 'West'),
            'AMER',
            sheetload_mapping_sdr_sfdc_bamboohr.sdr_region
        ) as sdr_region_grouped,
        ifnull(
            sheetload_mapping_sdr_sfdc_bamboohr.sdr_order_type, 'Other'
        ) as sdr_order_type,
        case
            when day(sdr.start_date) < 14
            then d_1.last_day_of_month
            when day(sdr.start_date) >= 14
            then d_2.last_day_of_month
            else null
        end as sdr_ramp_end_date
    from sdr
    inner join
        sheetload_mapping_sdr_sfdc_bamboohr
        on sdr.employee_id = sheetload_mapping_sdr_sfdc_bamboohr.eeid
    left join
        dim_crm_user
        on dim_crm_user.dim_crm_user_id = sheetload_mapping_sdr_sfdc_bamboohr.user_id
    left join dim_date as d_1 on dateadd('month', 1, sdr.start_date) = d_1.date_actual
    left join dim_date as d_2 on dateadd('month', 2, sdr.start_date) = d_2.date_actual

),
dim_date_final as (

    select *
    from dim_date
    where first_day_of_month > '2020-11-01' and first_day_of_month <= current_date

),
final as (

    select
        dim_date_final.date_actual,
        dim_date_final.first_day_of_month,
        dim_date_final.last_day_of_month,
        dim_date_final.first_day_of_week,
        dim_date_final.last_day_of_week,
        dim_date_final.fiscal_quarter_name_fy,
        dim_date_final.last_day_of_fiscal_quarter,
        case
            when
                dim_date_final.date_actual >= start_date
                and dim_date_final.date_actual <= sdr_ramp_end_date
            then 'Ramping'
            when
                dim_date_final.date_actual >= start_date
                and dim_date_final.date_actual > sdr_ramp_end_date
                and dim_date_final.date_actual <= emp_end_date
            then 'Active'
            else null
        end as sdr_type,
        sdr_ramp.*
    from sdr_ramp
    inner join
        dim_date_final
        on dim_date_final.date_actual
        between sdr_ramp.start_date and sdr_ramp.company_or_role_end_date

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@rkohnke",
        updated_by="@rkohnke",
        created_date="2022-01-20",
        updated_date="2022-01-25",
    )
}}
