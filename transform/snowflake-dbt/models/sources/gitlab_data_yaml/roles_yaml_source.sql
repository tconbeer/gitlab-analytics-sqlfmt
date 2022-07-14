with
    source as (

        select
            *,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "roles") }}

    ),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    intermediate_role_information as (

        select
            snapshot_date,
            data_by_row['title']::varchar as title,
            data_by_row['levels']::varchar as role_levels,
            data_by_row['open']::varchar as is_open,
            data_by_row['salary']::varchar as previous_salary_value,
            data_by_row['ic_ttc'] as ic_values,
            data_by_row['manager_ttc'] as manager_values,
            data_by_row['director_ttc'] as director_values,
            data_by_row['senior_director_ttc'] as senior_director_values,
            rank
        from intermediate

    ),
    renamed as (

        select
            snapshot_date,
            title,
            role_levels,
            try_to_boolean(is_open) as is_open,
            try_to_numeric(previous_salary_value) as previous_salary_value,
            try_to_numeric(
                ic_values['compensation']::varchar
            ) as individual_contributor_compensation,
            try_to_numeric(
                ic_values['percentage_variable']::varchar, 5, 2
            ) as individual_contributor_percentage_variable,
            try_to_boolean(
                ic_values['from_base']::varchar
            ) as individual_contributor_from_base,
            try_to_numeric(
                manager_values['compensation']::varchar
            ) as manager_compensation,
            try_to_numeric(
                manager_values['percentage_variable']::varchar
            ) as manager_percentage_variable,
            try_to_boolean(manager_values['from_base']::varchar) as manager_from_base,
            try_to_numeric(
                director_values['compensation']::varchar
            ) as director_compensation,
            try_to_numeric(
                director_values['percentage_variable']::varchar, 5, 2
            ) as director_percentage_variable,
            try_to_boolean(director_values['from_base']::varchar) as director_from_base,
            try_to_numeric(
                senior_director_values['compensation']::varchar
            ) as senior_director_compensation,
            try_to_numeric(
                senior_director_values['percentage_variable']::varchar, 5, 2
            ) as senior_director_percentage_variable,
            try_to_boolean(
                senior_director_values['from_base']::varchar
            ) as senior_director_from_base,
            rank
        from intermediate_role_information

    )

select *
from renamed
