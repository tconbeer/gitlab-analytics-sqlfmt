with
    source as (select * from {{ source("greenhouse", "approvals") }}),
    renamed as (

        select

            -- keys
            offer_id::number as offer_id,
            application_id::number as application_id,
            job_id::number as job_id,
            candidate_id::number as candidate_id,
            approver_id::number as approver_id,
            group_id::number as group_id,

            -- info
            approval_type::varchar as approval_type,
            status::varchar as approval_status,
            version::number as approval_version,
            final_version::number as approval_version_final,
            group_order::number as group_order,
            group_quorum::number as group_quorum,
            assigned::timestamp as approval_assigned_at,
            completed::timestamp as approval_completed_at,
            created_at::timestamp as approval_created_at,
            updated_at::timestamp as approval_updated_at

        from source

    )

select *
from renamed
