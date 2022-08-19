with
    source as (

        select * from {{ ref("engineering_part_of_product_merge_requests_source") }}

    ),
    mr_information as (

        select *
        from {{ ref("gitlab_dotcom_merge_requests_xf") }}
        where
            array_contains('database::approved'::variant, labels)
            and merged_at is not null
            and project_id = 278964  -- where the db schema is

    ),
    changes_to_db_structure as (

        select distinct
            'gitlab.com' || plain_diff_url_path as mr_path,
            mr_information.merge_request_updated_at,
            merged_at
        from source
        inner join lateral flatten(input => file_diffs, outer => true) d
        inner join
            mr_information
            on source.product_merge_request_iid = mr_information.merge_request_iid
        where
            target_branch_name = 'master' and d.value['file_path'] = 'db/structure.sql'

    )

select *
from changes_to_db_structure
order by merged_at desc
