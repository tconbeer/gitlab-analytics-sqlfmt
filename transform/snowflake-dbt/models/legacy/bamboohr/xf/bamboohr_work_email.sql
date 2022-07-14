with
    -- mapping on bamboohr_id_employee_number_mapping as this has accounted for all
    -- hired employees whereas bamboohr_directory_source has not
    mapping as (select * from {{ ref("bamboohr_id_employee_number_mapping") }}),
    bamboohr_directory as (

        select *
        from {{ ref("bamboohr_directory_source") }}
        qualify
            row_number() over (
                partition by employee_id, date_trunc(day, uploaded_at)
                order by uploaded_at desc
            )
            = 1

    ),
    intermediate as (

        select
            bamboohr_directory.*,
            last_value(date_trunc(day, bamboohr_directory.uploaded_at)) over
            (partition by bamboohr_directory.employee_id order by uploaded_at
            ) as max_uploaded_date,
            dense_rank() over (
                partition by bamboohr_directory.employee_id order by uploaded_at desc
            ) as rank_email_desc
        from mapping
        left join
            bamboohr_directory on mapping.employee_id = bamboohr_directory.employee_id
        qualify
            row_number() over (
                partition by bamboohr_directory.employee_id, work_email
                order by uploaded_at
            )
            = 1

    ),
    final as (

        select
            employee_id,
            full_name,
            work_email,
            uploaded_at as valid_from_date,
            iff(
                max_uploaded_date < current_date() and rank_email_desc = 1,
                max_uploaded_date,
                coalesce(
                    lead(dateadd(day, -1, uploaded_at)) over (
                        partition by employee_id order by uploaded_at
                    ),
                    {{ max_date_in_bamboo_analyses() }}
                )
            ) as valid_to_date,
            dense_rank() over (
                partition by employee_id order by valid_from_date desc
            ) as rank_email_desc
        from intermediate
    )

select *
from final
