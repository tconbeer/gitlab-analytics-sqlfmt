{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("netsuite", "transaction_lines") }}),
    renamed as (

        select
            {{ dbt_utils.surrogate_key(["transaction_id", "transaction_line_id"]) }}
            as transaction_lines_unique_id,
            -- Primary Key
            transaction_id::float as transaction_id,
            transaction_line_id::float as transaction_line_id,

            -- Foreign Keys
            account_id::float as account_id,
            class_id::float as class_id,
            department_id::float as department_id,
            subsidiary_id::float as subsidiary_id,
            company_id::float as company_id,

            -- info
            memo::varchar as memo,
            receipt_url::varchar as receipt_url,
            amount::float as amount,
            gross_amount::float as gross_amount,

            lower(non_posting_line)::varchar as non_posting_line

        from source

    )

select *
from renamed
