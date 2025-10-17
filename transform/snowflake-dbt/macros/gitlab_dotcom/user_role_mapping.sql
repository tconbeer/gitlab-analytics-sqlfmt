{%- macro user_role_mapping(user_role) -%}

    case
        when {{ user_role }} = 0
        then 'Software Developer'
        when {{ user_role }} = 1
        then 'Development Team Lead'
        when {{ user_role }} = 2
        then 'Devops Engineer'
        when {{ user_role }} = 3
        then 'Systems Administrator'
        when {{ user_role }} = 4
        then 'Security Analyst'
        when {{ user_role }} = 5
        then 'Data Analyst'
        when {{ user_role }} = 6
        then 'Product Manager'
        when {{ user_role }} = 7
        then 'Product Designer'
        when {{ user_role }} = 8
        then 'Other'
        when {{ user_role }} = 99
        then 'Experiment Default Value - Signup Not Completed'
        else null
    end

{%- endmacro -%}
