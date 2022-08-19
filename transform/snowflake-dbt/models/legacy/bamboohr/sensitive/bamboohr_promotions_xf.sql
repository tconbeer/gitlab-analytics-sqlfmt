with
    bamboohr_compensation as (select * from {{ ref("bamboohr_compensation_source") }}),
    bamboohr_compensation_changes as (

        select
            *,
            row_number() over (
                partition by employee_id order by effective_date
            ) as rank_by_effective_date,
            row_number() over (
                partition by employee_id order by compensation_update_id
            ) as rank_by_id,
            case
                when rank_by_effective_date != rank_by_id
                then
                    lag(compensation_value) over (
                        partition by employee_id order by effective_date
                    )
                else
                    lag(compensation_value) over (
                        partition by employee_id order by compensation_update_id
                    )
            end as prior_compensation_value,
            case
                when rank_by_effective_date != rank_by_id
                then
                    lag(compensation_currency) over (
                        partition by employee_id order by effective_date
                    )
                else
                    lag(compensation_currency) over (
                        partition by employee_id order by compensation_update_id
                    )
            end as prior_compensation_currency,
            row_number() over (
                partition by employee_id, effective_date order by compensation_update_id
            ) as rank_compensation_change_effective_date
        from bamboohr_compensation

    ),
    pay_frequency as (

        select
            *,
            row_number() over (
                partition by employee_id order by effective_date
            ) as pay_frequency_row_number
        from {{ ref("bamboohr_job_role") }}
        where pay_frequency is not null

    ),
    ote as (

        select
            *,
            row_number() over (
                partition by employee_id, effective_date
                order by target_earnings_update_id
            ) as rank_ote_effective_date
        from {{ ref("bamboohr_ote_source") }}

    ),
    employee_directory as (select * from {{ ref("employee_directory_intermediate") }}),
    currency_conversion as (

        select
            *,
            lag(currency_conversion_factor) over (
                partition by employee_id order by conversion_id
            ) as prior_conversion_factor,
            row_number() over (
                partition by employee_id order by conversion_id
            ) as rank_conversion_id
        from {{ ref("bamboohr_currency_conversion_source") }}

    ),
    currency_conversion_factor_periods as (

        select
            *,
            lead(annual_amount_usd_value) over (
                partition by employee_id order by conversion_id
            ) as next_usd_value,
            lead(dateadd(day, -1, effective_date)) over (
                partition by employee_id order by conversion_id
            ) as next_effective_date
        from currency_conversion
        where
            currency_conversion_factor <> prior_conversion_factor
            or rank_conversion_id = 1

    ),
    joined as (

        select
            employee_directory.employee_number,
            employee_directory.full_name,
            bamboohr_compensation_changes.*,
            employee_directory.division_mapped_current as division,
            employee_directory.division_grouping,
            employee_directory.department_modified as department,
            employee_directory.department_grouping,
            employee_directory.job_title,
            case
                when
                    bamboohr_compensation_changes.employee_id
                    in (40955, 40647, 41234, 40985, 41027, 40782, 40540)
                    and bamboohr_compensation_changes.effective_date <= '2020-06-01'
                then 12
                -- we didn't capture pay frequency prior to 2020.07 and in 2020.07 the
                -- pay frequency had changed for these individuals
                when
                    bamboohr_compensation_changes.employee_id = '40874'
                    and bamboohr_compensation_changes.effective_date < '2019-12-31'
                then 12
                -- This team member has a pay frequency of 12 prior to the 2019.12.31
                -- and the current pay frequency for 2020
                else
                    coalesce(
                        pay_frequency.pay_frequency, pay_frequency_initial.pay_frequency
                    )
            end as pay_frequency,
            currency_conversion_factor,
            ote.variable_pay,
            ote.annual_amount_usd_value as ote_usd,
            ote.prior_annual_amount_usd as prior_ote_usd,
            ote.change_in_annual_amount_usd as ote_change,
            rank_ote_effective_date,
            currency_conversion_factor_periods.annual_amount_usd_value,
            currency_conversion_factor_periods.next_usd_value
        from bamboohr_compensation_changes
        left join
            employee_directory
            on bamboohr_compensation_changes.employee_id
            = employee_directory.employee_id
            and bamboohr_compensation_changes.effective_date
            = employee_directory.date_actual
        left join
            pay_frequency
            on bamboohr_compensation_changes.employee_id = pay_frequency.employee_id
            and bamboohr_compensation_changes.effective_date
            between pay_frequency.effective_date and pay_frequency.next_effective_date
        left join
            pay_frequency as pay_frequency_initial
            on bamboohr_compensation_changes.employee_id
            = pay_frequency_initial.employee_id
            and pay_frequency_initial.pay_frequency_row_number = 1
        left join
            currency_conversion_factor_periods
            on bamboohr_compensation_changes.employee_id
            = currency_conversion_factor_periods.employee_id
            and bamboohr_compensation_changes.effective_date
            between currency_conversion_factor_periods.effective_date and coalesce(
                currency_conversion_factor_periods.next_effective_date, current_date()
            )
        left join
            ote
            on bamboohr_compensation_changes.employee_id = ote.employee_id
            and bamboohr_compensation_changes.effective_date = ote.effective_date
            and bamboohr_compensation_changes.rank_compensation_change_effective_date
            = ote.rank_ote_effective_date

    ),
    intermediate as (

        select
            compensation_update_id,
            employee_number,
            employee_id,
            full_name,
            division,
            division_grouping,
            department,
            department_grouping,
            job_title,
            compensation_change_reason,
            effective_date,
            currency_conversion_factor,
            lag(currency_conversion_factor) over (
                partition by employee_id order by compensation_update_id
            ) as prior_currency_conversion_factor,
            pay_frequency,
            lag(pay_frequency) over (
                partition by employee_id order by compensation_update_id
            ) as prior_pay_frequency,
            compensation_value as new_compensation_value,
            prior_compensation_value as prior_compensation_value,
            compensation_currency as new_compensation_currency,
            prior_compensation_currency,
            variable_pay,
            ote_usd,
            prior_ote_usd,
            ote_change,
            next_usd_value,
            annual_amount_usd_value
        from joined

    ),
    promotions as (

        select
            compensation_update_id,
            effective_date as promotion_date,
            date_trunc(month, effective_date) as promotion_month,
            employee_number,
            employee_id,
            full_name,
            division,
            division_grouping,
            department,
            department_grouping,
            job_title,
            variable_pay,
            iff(
                compensation_update_id = 21917,
                next_usd_value,
                new_compensation_value * pay_frequency * currency_conversion_factor
            ) as new_compensation_value_usd,
            case
                when compensation_update_id = 21917
                then annual_amount_usd_value
                when new_compensation_currency = prior_compensation_currency
                then
                    prior_compensation_value
                    * prior_pay_frequency
                    * currency_conversion_factor
                else
                    prior_compensation_value
                    * prior_pay_frequency
                    * prior_currency_conversion_factor
            end as prior_compensation_value_usd,
            new_compensation_value_usd
            - prior_compensation_value_usd as change_in_comp_usd,
            coalesce(ote_usd, 0) as ote_usd,
            coalesce(prior_ote_usd, 0) as prior_ote_usd,
            coalesce(ote_change, 0) as ote_change,
            iff(  -- -incorrectly labeled 
                compensation_update_id = 20263,
                null,
                coalesce(ote_change, 0) + change_in_comp_usd
            ) as total_change_in_comp,
            iff(
                compensation_update_id = 20263,
                null,
                round(
                    (coalesce(ote_change, 0) + change_in_comp_usd)
                    / (prior_compensation_value_usd + coalesce(prior_ote_usd, 0)),
                    2
                )
            ) as percent_change_in_comp
        from intermediate
        where compensation_change_reason = 'Promotion' and job_title not like '%VP%'

    )

select *
from promotions
