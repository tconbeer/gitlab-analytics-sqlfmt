with
    source as (select * from {{ ref("gitlab_dotcom_pages_domains_dedupe_source") }}),
    renamed as (

        select

            id::number as pages_domain_id,
            project_id::number as project_id,
            verified_at::timestamp as verified_at,
            verification_code::varchar as verification_code,
            enabled_until::timestamp as enabled_until,
            remove_at::timestamp as remove_at,
            auto_ssl_enabled::boolean as is_auto_ssl_enabled,
            certificate_valid_not_before::timestamp as certificate_valid_not_before,
            certificate_valid_not_after::timestamp as certificate_valid_not_after,
            certificate_source::varchar as certificate_source

        from source

    )

select *
from renamed
