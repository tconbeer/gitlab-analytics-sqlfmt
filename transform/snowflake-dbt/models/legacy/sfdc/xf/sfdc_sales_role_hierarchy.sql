with recursive
    managers as (

        select
            user_id, name, role_name, manager_name, manager_id, 0 as level, '' as path
        from {{ ref("sfdc_users_xf") }}
        where role_name = 'CRO'

        UNION ALL

        select
            users.user_id,
            users.name,
            users.role_name,
            users.manager_name,
            users.manager_id,
            level + 1,
            path || managers.role_name || '::'
        from {{ ref("sfdc_users_xf") }} users
        inner join managers on users.manager_id = managers.user_id

    ),
    final as (

        select
            user_id,
            name,
            role_name,
            manager_name,
            split_part(path, '::', 1)::varchar as parent_role_1,
            split_part(path, '::', 2)::varchar as parent_role_2,
            split_part(path, '::', 3)::varchar as parent_role_3,
            split_part(path, '::', 4)::varchar as parent_role_4,
            split_part(path, '::', 5)::varchar as parent_role_5
        from managers

    )

select *
from final
