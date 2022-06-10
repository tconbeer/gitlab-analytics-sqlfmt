with recursive
    sfdc_account_source as (select * from {{ ref("sfdc_account_source") }}),
    recursive_cte(account_id, master_record_id, is_deleted, lineage) as (

        select account_id, master_record_id, is_deleted, to_array(account_id) as lineage
        from sfdc_account_source
        where master_record_id is null

        UNION ALL

        select
            iter.account_id,
            iter.master_record_id,
            iter.is_deleted,
            array_insert(anchor.lineage, 0, iter.account_id) as lineage
        from recursive_cte as anchor
        inner join
            sfdc_account_source as iter on iter.master_record_id = anchor.account_id

    ),
    final as (

        select
            account_id as sfdc_account_id,
            lineage[array_size(lineage) - 1]::varchar as merged_account_id,
            is_deleted,
            iff(merged_account_id != account_id, true, false) as is_merged,
            iff(is_deleted and not is_merged, true, false) as deleted_not_merged,
            -- return final common dimension mapping,
            iff(deleted_not_merged, '-1', merged_account_id) as dim_crm_account_id
        from recursive_cte

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@mcooperDD",
            updated_by="@mcooperDD",
            created_date="2020-11-23",
            updated_date="2020-11-23",
        )
    }}
