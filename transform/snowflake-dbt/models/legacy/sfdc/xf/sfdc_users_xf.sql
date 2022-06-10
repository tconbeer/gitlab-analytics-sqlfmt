with recursive
    users as (select * from {{ ref("sfdc_users") }}),
    user_role as (select * from {{ ref("sfdc_user_roles") }}),
    base as (

        select
            users.name as name,
            users.department as department,
            users.title as title,
            users.team,  -- team,
            users.user_id,  -- user_id
            case  -- only expose GitLab.com email addresses of internal employees
                when users.user_email like '%gitlab.com' then users.user_email else null
            end as user_email,
            manager.name as manager_name,
            manager.user_id as manager_id,
            user_role.name as role_name,
            users.start_date,  -- start_date
            users.is_active  -- is_active
        from users
        left outer join user_role on users.user_role_id = user_role.id
        left outer join users as manager on manager.user_id = users.manager_id

    ),
    managers as (

        select
            user_id, name, role_name, manager_name, manager_id, 0 as level, '' as path
        from base
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
        from base users
        inner join managers on users.manager_id = managers.user_id

    ),
    cro_sfdc_hierarchy as (

        select
            user_id,
            name,
            role_name,
            manager_name,
            split_part(path, '::', 1)::varchar(50) as level_1,
            split_part(path, '::', 2)::varchar(50) as level_2,
            split_part(path, '::', 3)::varchar(50) as level_3,
            split_part(path, '::', 4)::varchar(50) as level_4,
            split_part(path, '::', 5)::varchar(50) as level_5
        from managers

    ),
    final as (
        select
            base.*,

            -- account owner hierarchies levels
            trim(cro.level_2) as sales_team_level_2,
            trim(cro.level_3) as sales_team_level_3,
            trim(cro.level_4) as sales_team_level_4,
            case
                when trim(cro.level_2) is not null then trim(cro.level_2) else 'n/a'
            end as sales_team_vp_level,
            case
                when (trim(cro.level_3) is not null and trim(cro.level_3) != '')
                then trim(cro.level_3)
                else 'n/a'
            end as sales_team_rd_level,
            case
                when cro.level_3 like 'ASM%'
                then cro.level_3
                when cro.level_4 like 'ASM%' or cro.level_4 like 'Area Sales%'
                then cro.level_4
                else 'n/a'
            end as sales_team_asm_level,
            case
                when
                    (
                        cro.level_4 is not null and cro.level_4 != '' and (
                            cro.level_4 like 'ASM%' or cro.level_4 like 'Area Sales%'
                        )
                    )
                then cro.level_4
                when (cro.level_3 is not null and cro.level_3 != '')
                then cro.level_3
                when (cro.level_2 is not null and cro.level_2 != '')
                then cro.level_2
                else 'n/a'
            end as sales_min_hierarchy_level,
            case
                when sales_min_hierarchy_level in ('ASM - APAC - Japan', 'RD APAC')
                then 'APAC'
                when
                    sales_min_hierarchy_level in (
                        'ASM - Civilian',
                        'ASM - DoD - USAF+COCOMS+4th Estate',
                        'ASM - NSG',
                        'ASM - SLED',
                        'ASM-DOD- Army+Navy+Marines+SI''s',
                        'CD PubSec',
                        'RD PubSec'
                    )
                then 'PUBSEC'
                when
                    sales_min_hierarchy_level in (
                        'ASM - EMEA - DACH',
                        'ASM - EMEA - North',
                        'ASM - MM - EMEA',
                        'ASM-SMB-EMEA',
                        'CD EMEA',
                        'RD EMEA'
                    )
                then 'EMEA'
                when
                    sales_min_hierarchy_level in (
                        'ASM - MM - East',
                        'ASM - US East - Southeast',
                        'ASM-SMB-AMER-East',
                        'Area Sales Manager - US East - Central',
                        'Area Sales Manager - US East - Named Accounts',
                        'Area Sales Manager - US East - Northeast',
                        'RD US East'
                    )
                then 'US East'
                when
                    sales_min_hierarchy_level in (
                        'ASM - MM - West',
                        'ASM - US West - NorCal',
                        'ASM - US West - PacNW',
                        'ASM - US West - SoCal+Rockies',
                        'ASM-SMB-AMER-West',
                        'RD US West'
                    )
                then 'US West'
                else 'n/a'
            end as sales_region,
            case when cro.level_2 like 'VP%' then 1 else 0 end as is_lvl_2_vp_flag
        from base
        left join cro_sfdc_hierarchy cro on cro.user_id = base.user_id
    )

select *
from final
