{%- macro it_job_title_hierarchy(job_title) -%}

case
    when
        lower({{ pad_column(job_title) }}) like any (
            '%head% it%',
            '%vp%technology%',
            '%director%technology%',
            '%director%engineer%',
            '%chief%information%',
            '%chief%technology%',
            '%president%technology%',
            '%vp%technology%',
            '%director%development%',
            '% it%director%',
            '%director%information%',
            '%director% it%',
            '%chief%engineer%',
            '%director%quality%',
            '%vp%engineer%',
            '%head%information%',
            '%vp%information%',
            '%president%information%',
            '%president%engineer%',
            '%president%development%',
            '%director% it%',
            '%engineer%director%',
            '%head%engineer%',
            '%engineer%head%',
            '%chief%software%',
            '%director%procurement%',
            '%procurement%director%',
            '%head%procurement%',
            '%procurement%head%',
            '%chief%procurement%',
            '%vp%procurement%',
            '%procurement%vp%',
            '%president%procurement%',
            '%procurement%president%',
            '%head%devops%'
        )
        or array_contains('cio'::variant, split(lower({{ job_title }}), ' '))
        or array_contains('cio'::variant, split(lower({{ job_title }}), ','))
        or array_contains('cto'::variant, split(lower({{ job_title }}), ' '))
        or array_contains('cto'::variant, split(lower({{ job_title }}), ','))
        or array_contains('cfo'::variant, split(lower({{ job_title }}), ' '))
        or array_contains('cfo'::variant, split(lower({{ job_title }}), ','))
    then 'IT Decision Maker'

    when
        lower({{ pad_column(job_title) }}) like any (
            '%manager%information%',
            '%manager%technology%',
            '%database%administrat%',
            '%manager%engineer%',
            '%engineer%manager%',
            '%information%manager%',
            '%technology%manager%',
            '%manager%development%',
            '%manager%quality%',
            '%manager%network%',
            '% it%manager%',
            '%manager% it%',
            '%manager%systems%',
            '%manager%application%',
            '%technical%manager%',
            '%manager%technical%',
            '%manager%infrastructure%',
            '%manager%implementation%',
            '%devops%manager%',
            '%manager%devops%',
            '%manager%software%',
            '%procurement%manager%',
            '%manager%procurement%'
        )
        and not array_contains('project'::variant, split(lower({{ job_title }}), ' '))
    then 'IT Manager'

    when
        lower({{ pad_column(job_title) }}) like any (
            '% it %',
            '% it,%',
            '%infrastructure%',
            '%engineer%',
            '%techno%',
            '%information%',
            '%developer%',
            '%database%',
            '%solutions architect%',
            '%system%',
            '%software%',
            '%technical lead%',
            '%programmer%',
            '%network administrat%',
            '%application%',
            '%procurement%',
            '%development%',
            '%tech%lead%'
        )
    then 'IT Individual Contributor'

    else null

end as it_job_title_hierarchy

{%- endmacro -%}
