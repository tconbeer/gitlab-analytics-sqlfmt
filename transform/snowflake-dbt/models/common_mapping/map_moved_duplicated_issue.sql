with recursive
    issues as (select * from {{ ref("gitlab_dotcom_issues_source") }}),
    issues_moved_duplicated as (

        select *, ifnull(moved_to_id, duplicated_to_id) as moved_duplicated_to_id
        from issues

    ),
    recursive_cte(issue_id, moved_duplicated_to_id, issue_lineage) as (

        select issue_id, moved_duplicated_to_id, to_array(issue_id) as issue_lineage
        from issues_moved_duplicated
        where moved_duplicated_to_id is null

        union all

        select
            iter.issue_id,
            iter.moved_duplicated_to_id,
            array_insert(anchor.issue_lineage, 0, iter.issue_id) as issue_lineage
        from recursive_cte as anchor
        inner join
            issues_moved_duplicated as iter
            on iter.moved_duplicated_to_id = anchor.issue_id

    ),
    final as (

        select
            issue_id as issue_id,
            issue_lineage as issue_lineage,
            issue_lineage[
                array_size(issue_lineage) - 1
            ]::number as last_moved_duplicated_issue_id,
            iff(
                last_moved_duplicated_issue_id != issue_id, true, false
            ) as is_issue_moved_duplicated,
            -- return final common dimension mapping,
            last_moved_duplicated_issue_id as dim_issue_id
        from recursive_cte

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-10-12",
            updated_date="2021-10-12",
        )
    }}
