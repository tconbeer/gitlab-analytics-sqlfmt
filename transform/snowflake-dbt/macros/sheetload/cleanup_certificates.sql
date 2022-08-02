{%- macro cleanup_certificates(certificate_name) -%}

clean_score as (

    select
        completed_date,
        submitter_name,
        trim(split_part(score, '/', 1))::number as correct_responses,
        trim(split_part(score, '/', 2))::number as total_responses,
        case
            when lower(submitter_email) like '%@gitlab.com%' then true else false
        end as is_team_member,
        case
            when lower(submitter_email) like '%@gitlab.com%'
            then trim(lower(submitter_email))
            else md5(submitter_email)
        end as submitter_email,
        {{ certificate_name }} as certificate_name,
        last_updated_at
    from source

)

select *
from clean_score

{%- endmacro -%}
