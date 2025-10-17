{%- macro include_gitlab_email(column_name) -%}

    case
        when {{ column_name }} is null
        then 'Exclude'
        when {{ column_name }} like '%-%'
        then 'Exclude'  -- removes any emails with special character - 
        when {{ column_name }} like '%~%'
        then 'Exclude'  -- removes emails with special character ~
        when {{ column_name }} like '%+%'
        then 'Exclude'  -- removes any emails with special character + 
        when {{ column_name }} like '%admin%'
        then 'Exclude'  -- removes records with the word admin
        when {{ column_name }} like '%hack%'
        then 'Exclude'  -- removes hack accounts
        when {{ column_name }} like '%xxx%'
        then 'Exclude'  -- removes accounts with more than three xs
        when {{ column_name }} like '%gitlab%'
        then 'Exclude'  -- removes accounts that have the word gitlab
        when {{ column_name }} like '%test%'
        then 'Exclude'  -- removes accounts with test in the name
        when
            {{ column_name }} in (  -- removes duplicate emails 
                'mckai.javeion', 'deandre', 'gopals', 'kenny', 'jason'
            )
        then 'Exclude'
        else 'Include'
    end

{%- endmacro -%}
