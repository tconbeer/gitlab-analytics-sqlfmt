with
    source as (select * from {{ ref("gitlab_dotcom_namespaces_snapshots_base") }}),
    /*
This CTE finds groups of snapshoted chages that changed the parent id. This is a typical 'gaps and islands' problem. 
*/
    parent_groups as (
        select
            namespace_id,
            parent_id,
            ifnull(parent_id, -1) as no_null_parent_id,
            lag(no_null_parent_id, 1, -1) over (
                partition by namespace_id order by valid_from
            ) as lag_parent_id,
            conditional_true_event(no_null_parent_id != lag_parent_id) over (
                partition by namespace_id order by valid_from
            ) as parent_id_group,
            valid_from,
            ifnull(valid_to, current_date()) as valid_to
        from source
    ),
    parent_change as (
        select
            namespace_id,
            parent_id,
            parent_id_group,
            min(valid_from) as valid_from,
            max(valid_to) as valid_to
        from parent_groups
        group by 1, 2, 3
    ),
    recursive_namespace_lineage(
        namespace_id,
        parent_id,
        valid_to,
        valid_from,
        valid_to_list,
        valid_from_list,
        upstream_lineage
    ) as (
        select
            root.namespace_id,
            root.parent_id,
            root.valid_to,
            root.valid_from,
            to_array(root.valid_to) as valid_to_list,
            to_array(root.valid_from) as valid_from_list,
            to_array(root.namespace_id) as upstream_lineage
        from parent_change as root
        where parent_id is null

        union all

        select
            iter.namespace_id,
            iter.parent_id,
            iter.valid_to,
            iter.valid_from,
            array_append(anchor.valid_to_list, iter.valid_to) as valid_to_list,
            array_append(anchor.valid_from_list, iter.valid_from) as valid_from_list,
            array_append(anchor.upstream_lineage, iter.namespace_id) as upstream_lineage
        from recursive_namespace_lineage as anchor
        inner join
            parent_change as iter
            on iter.parent_id = anchor.namespace_id
            and not array_contains(iter.namespace_id::variant, anchor.upstream_lineage)
            and (
                case
                    when iter.valid_from between anchor.valid_from and anchor.valid_to
                    then true
                    when iter.valid_to between anchor.valid_from and anchor.valid_to
                    then true
                    when anchor.valid_from between iter.valid_from and iter.valid_to
                    then true
                    else false
                end
            )
            = true
    ),

    namespace_lineage_scd as (
        select
            recursive_namespace_lineage.namespace_id,
            recursive_namespace_lineage.parent_id,
            recursive_namespace_lineage.upstream_lineage,
            recursive_namespace_lineage.upstream_lineage[0]::number
            as ultimate_parent_id,
            array_size(recursive_namespace_lineage.upstream_lineage) as lineage_depth,
            recursive_namespace_lineage.valid_from_list,
            recursive_namespace_lineage.valid_to_list,
            max(from_list.value::timestamp) as lineage_valid_from,
            min(to_list.value::timestamp) as lineage_valid_to
        from recursive_namespace_lineage
        inner join lateral flatten(input => valid_from_list) from_list
        inner join lateral flatten(input => valid_to_list) to_list
        group by 1, 2, 3, 4, 5, 6, 7
        having lineage_valid_to > lineage_valid_from
    ),
    event_index as (
        select
            {{ dbt_utils.surrogate_key(["namespace_id", "lineage_valid_from"]) }}
            as namespace_lineage_id,
            namespace_id,
            parent_id,
            upstream_lineage,
            ultimate_parent_id,
            lineage_depth,
            lineage_valid_from,
            lineage_valid_to,
            row_number() over (
                partition by namespace_id order by lineage_valid_from
            ) as sequence_number,
            iff(lineage_valid_to = current_date(), true, false) as is_current
        from namespace_lineage_scd
    )

    {{
        dbt_audit(
            cte_ref="event_index",
            created_by="@pempey",
            updated_by="@pempey",
            created_date="2021-11-16",
            updated_date="2021-11-16",
        )
    }}
