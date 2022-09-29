with
    source as (select * from {{ source("sheetload", "gcp_active_cud") }}),
    renamed as (
        select
            start_date::date as start_date,
            end_date::date as end_date,
            vcpus::number as vcpus,
            ram::number as ram,
            commit_term::varchar as commit_term,
            region::varchar as region,
            machine_type::varchar as machine_type,
            is_active::varchar as is_active,
            hourly_commit_vcpus::number as hourly_commit_vcpus,
            hourly_commit_ram::number as hourly_commit_ram,
            total_hourly_commit::number as total_hourly_commit,
            daily_commit_amount_vcpus::number as daily_commit_amount_vcpus,
            daily_commit_amount_ram::number as daily_commit_amount_ram,
            daily_total_commit::number as daily_total_commit
        from source
    )

select *
from renamed
