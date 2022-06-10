{%- macro include_gitlab_email(column_name) -%}

case
    when {{ column_name }} is null
    then 'Exclude'
    when {{ column_name }} like '%-%'  -- removes any emails with special character - 
    then 'Exclude'
    when {{ column_name }} like '%~%'  -- removes emails with special character ~
    then 'Exclude'
    when {{ column_name }} like '%+%'  -- removes any emails with special character + 
    then 'Exclude'
    when {{ column_name }} like '%admin%'  -- removes records with the word admin
    then 'Exclude'
    when {{ column_name }} like '%hack%'  -- removes hack accounts
    then 'Exclude'
    when {{ column_name }} like '%xxx%'  -- removes accounts with more than three xs
    then 'Exclude'
    -- removes accounts that have the word gitlab
    when {{ column_name }} like '%gitlab%'
    then 'Exclude'
    when {{ column_name }} like '%test%'  -- removes accounts with test in the name
    then 'Exclude'
    -- removes duplicate emails 
    when {{ column_name }} in ('mckai.javeion', 'deandre', 'gopals', 'kenny', 'jason')
    then 'Exclude'
    else 'Include'
end

{%- endmacro -%}
