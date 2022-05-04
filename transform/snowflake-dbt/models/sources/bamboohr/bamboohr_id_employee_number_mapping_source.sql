with
    source as (

        select *
        from {{ source("bamboohr", "id_employee_number_mapping") }}
        qualify
            row_number() over (
                partition by date_trunc(day, uploaded_at) order by uploaded_at desc
            ) = 1

    ),
    intermediate as (

        select
            nullif(d.value['employeeNumber'], '')::number as employee_number,
            d.value['id']::number as employee_id,
            d.value['firstName']::varchar as first_name,
            d.value['lastName']::varchar as last_name,
            (
                case
                    when d.value['hireDate'] = ''
                    then null
                    when d.value['hireDate'] = '0000-00-00'
                    then null
                    else d.value['hireDate']::varchar
                end
            )::date as hire_date,
            (
                case
                    when d.value['terminationDate'] = ''
                    then null
                    when d.value['terminationDate'] = '0000-00-00'
                    then null
                    else d.value['terminationDate']::varchar
                end
            )::date as termination_date,
            d.value['customCandidateID']::number as greenhouse_candidate_id,
            d.value['customCostCenter']::varchar as cost_center,
            d.value['customGitLabUsername']::varchar as gitlab_username,
            d.value[
                'customJobTitleSpeciality'
            ]::varchar as jobtitle_speciality_single_select,
            d.value[
                'customJobTitleSpecialty(Multi-Select)'
            ]::varchar as jobtitle_speciality_multi_select,
            -- requiers cleaning becase of an error in the snapshoted source data
            case
                d.value['customLocality']::varchar
                when 'Canberra, Australia Capital Territory, Australia'
                then 'Canberra, Australian Capital Territory, Australia'
                else d.value['customLocality']::varchar
            end as locality,
            d.value['customNationality']::varchar as nationality,
            d.value['customOtherGenderOptions']::varchar as gender_dropdown,
            d.value['customRegion']::varchar as region,
            d.value['customRole']::varchar as job_role,
            d.value['customSalesGeoDifferential']::varchar as sales_geo_differential,
            d.value['dateofBirth']::varchar as date_of_birth,
            d.value['employeeStatusDate']::varchar as employee_status_date,
            d.value['employmentHistoryStatus']::varchar as employment_history_status,
            d.value['ethnicity']::varchar as ethnicity,
            d.value['gender']::varchar as gender,
            trim(d.value['country']::varchar) as country,
            d.value['age']::number as age,
            coalesce(
                d.value['customJobGrade']::varchar, d.value['4659.0']::varchar
            ) as job_grade,
            coalesce(
                d.value['customPayFrequency']::varchar, d.value['4657.0']::varchar
            ) as pay_frequency,
            uploaded_at::timestamp as uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext['employees']), outer => true) d

    ),
    final as (

        select
            *, dense_rank() over (order by uploaded_at desc) as uploaded_row_number_desc
        from intermediate
        where
            hire_date is not null and (
                lower(first_name) not like '%greenhouse test%' and lower(
                    last_name
                ) not like '%test profile%' and lower(last_name) != 'test-gitlab'
            ) and employee_id != 42039
        -- The same emplpyee can appear more than once in the same upload.
        qualify
            row_number() over (
                partition by employee_number, date_trunc(day, uploaded_at)
                order by uploaded_at desc
            ) = 1

    )



select *
from final
