with
    source as (select * from {{ source("xactly", "xc_participant") }}),

    renamed as (

        select

            participant_id::float as participant_id,
            version::float as version,
            name::varchar as name,
            descr::varchar as descr,
            region::varchar as region,
            participant_type::varchar as participant_type,
            prefix::varchar as prefix,
            first_name::varchar as first_name,
            middle_name::varchar as middle_name,
            last_name::varchar as last_name,
            employee_id::varchar as employee_id,
            salary::float as salary,
            salary_unit_type_id::float as salary_unit_type_id,
            hire_date::varchar as hire_date,
            termination_date::varchar as termination_date,
            personal_target::float as personal_target,
            pr_target_unit_type_id::float as pr_target_unit_type_id,
            native_cur_unit_type_id::float as native_cur_unit_type_id,
            is_active::varchar as is_active,
            user_id::float as user_id,
            created_date::varchar as created_date,
            created_by_id::float as created_by_id,
            created_by_name::varchar as created_by_name,
            modified_date::varchar as modified_date,
            modified_by_id::float as modified_by_id,
            modified_by_name::varchar as modified_by_name,
            entity::varchar as entity,
            adp__file__number::varchar as adp_file_number,
            sa__team::varchar as sa_team,
            position__category::varchar as position_category,
            account__owner__team::varchar as account_owner_team,
            ramping::varchar as ramping,
            segment::varchar as segment,
            department::varchar as department,
            bhr_eeid::varchar as bhr_eeid,
            bhr__title::varchar as bhr_title,
            bhr__emp_id::varchar as bhr_empid,
            effective__date::varchar as effective_date,
            annualized__variable::float as annualized_variable,
            personal__target__local::float as personal_target_local,
            salary__local::float as salary_local,
            annualized__variable__local::float as annualized_variable_local,
            semi__annual__variable__usd::float as semi_annual_variable_usd,
            start__date_in__role::varchar as start_date_in_role,
            annualized__variable__unit_type_id::float as annualized_variable_unittypeid,
            personal__target__local__unit_type_id::float
            as personal_target_local_unittypeid,
            salary__local__unit_type_id::float as salary_local_unittypeid,
            annualized__variable__local__unit_type_id::float
            as annualized_variable_local_unittypeid,
            semi__annual__variable__usd__unit_type_id::float
            as semi_annual_variable_usd_unittypeid,
            payment_cur_unit_type_id::float as payment_cur_unit_type_id,
            source_id::float as source_id,
            emp_status_id::float as emp_status_id,
            effective_start_date::varchar as effective_start_date,
            effective_end_date::varchar as effective_end_date,
            is_master::varchar as is_master,
            semi__annual__variable__local::float as semi_annual_variable_local,
            quarterly__variable__usd::float as quarterly_variable_usd,
            quarterly__variable__local::float as quarterly_variable_local,
            monthly__variable__local::float as monthly_variable_local,
            monthly__variable__usd::float as monthly_variable_usd,
            semi__annual__variable__local__unit_type_id::float
            as semi_annual_variable_local_unittypeid,
            quarterly__variable__usd__unit_type_id::float
            as quarterly_variable_usd_unittypeid,
            quarterly__variable__local__unit_type_id::float
            as quarterly_variable_local_unittypeid,
            monthly__variable__local__unit_type_id::float
            as monthly_variable_local_unittypeid,
            monthly__variable__usd__unit_type_id::float
            as monthly_variable_usd_unittypeid,
            business_group_id::float as business_group_id,
            obj_bonus_target::float as obj_bonus_target,
            obj_bonus_target_unittype_id::float as obj_bonus_target_unittype_id,
            obj_payment_cap_percent::float as obj_payment_cap_percent,
            is_obj_active::varchar as is_obj_active,
            prorated_salary::float as prorated_salary,
            prorated_personal_target::float as prorated_personal_target,
            version_reason_id::float as version_reason_id,
            version_sub_reason_id::float as version_sub_reason_id

        from source

    )

select *
from renamed
