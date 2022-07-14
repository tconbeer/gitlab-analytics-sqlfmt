with
    zuora_revenue_book as (

        select *
        from {{ source("zuora_revenue", "zuora_revenue_book") }}
        qualify rank() OVER (partition by id order by incr_updt_dt desc) = 1

    ),
    renamed as (

        select

            id::varchar as book_id,
            name::varchar as book_name,
            description::varchar as book_description,
            type::varchar as book_type,
            rc_prefix::varchar as revenue_contract_prefix,
            concat(open_prd_id::varchar, '01') as book_open_period_id,
            start_date::datetime as book_start_date,
            end_date::varchar as book_end_date,
            asst_segments::varchar as asset_segment,
            lblty_segments::varchar as liabilty_segment,
            allocation_flag::varchar as is_allocation,
            bndl_expl_flag::varchar as is_bundle_explosion,
            hard_freeze_flag::varchar as is_hard_freeze,
            ltst_enabled_flag::varchar as is_ltst_enabled,
            postable_flag::varchar as is_postable,
            primary_book_flag::varchar as is_primary_book,
            soft_freeze_flag::varchar as is_soft_freeze,
            client_id::varchar as client_id,
            concat(crtd_prd_id::varchar, '01') as book_created_period_id,
            crtd_dt::datetime as book_created_date,
            crtd_by::varchar as book_created_by,
            updt_dt::datetime as book_updated_date,
            updt_by::varchar as book_updated_by,
            incr_updt_dt::datetime as incremental_update_date,
            enabled_flag::varchar as is_enabled

        from zuora_revenue_book

    )

select *
from renamed
