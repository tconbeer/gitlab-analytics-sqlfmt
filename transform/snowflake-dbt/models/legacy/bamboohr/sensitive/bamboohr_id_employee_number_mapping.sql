with
    source as (

        select *
        from {{ ref("bamboohr_id_employee_number_mapping_source") }}
        where uploaded_row_number_desc = 1

    ),
    intermediate as (

        select
            employee_number,
            employee_id,
            first_name,
            last_name,
            hire_date,
            termination_date,
            case
                when age between 18 and 24
                then '18-24'
                when age between 25 and 29
                then '25-29'
                when age between 30 and 34
                then '30-34'
                when age between 35 and 39
                then '35-39'
                when age between 40 and 44
                then '40-44'
                when age between 44 and 49
                then '44-49'
                when age between 50 and 54
                then '50-54'
                when age between 55 and 59
                then '55-59'
                when age >= 60
                then '60+'
                when age is null
                then 'Unreported'
                when age = -1
                then 'Unreported'
                else null
            end as age_cohort,
            coalesce(gender_dropdown, gender, 'Did Not Identify') as gender,
            coalesce(ethnicity, 'Did Not Identify') as ethnicity,
            country,
            nationality,
            region,
            case
                when
                    region = 'Americas' and country in (
                        'United States', 'Canada', 'Mexico'
                    )
                then 'NORAM'
                when
                    region = 'Americas' and country not in (
                        'United States', 'Canada', 'Mexico'
                    )
                then 'LATAM'
                else region
            end as region_modified,
            iff(
                country = 'United States',
                coalesce(gender_dropdown, gender, 'Did Not Identify') || '_' || country,
                coalesce(gender_dropdown, gender, 'Did Not Identify') || '_' || 'Non-US'
            ) as gender_region,
            greenhouse_candidate_id,
            uploaded_at as last_updated_date,
            case
                when
                    coalesce(gender_dropdown, gender, 'Did Not Identify') not in (
                        'Male', 'Did Not Identify'
                    )
                then true
                when
                    coalesce(ethnicity, 'Did Not Identify') not in (
                        'White', 'Did Not Identify'
                    )
                then true
                else false
            end as urg_group
        from source
        where
            hire_date is not null or (
                lower(first_name) not like '%greenhouse test%' and lower(
                    last_name
                ) not like '%test profile%' and lower(last_name) != 'test-gitlab'
            ) or employee_id not in (42039, 42043)


    )

select *
from intermediate
