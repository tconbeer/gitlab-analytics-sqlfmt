{%- macro sales_hierarchy_sales_segment_cleaning(column_1) -%}

case
    when lower({{ column_1 }}) = 'smb'
    then 'SMB'
    when lower({{ column_1 }}) like ('mid%market')
    then 'Mid-Market'
    when lower({{ column_1 }}) = 'public sector'
    then 'PubSec'
    when lower({{ column_1 }}) = 'pubsec'
    then 'PubSec'
    when lower({{ column_1 }}) = 'unknown'
    then 'SMB'
    when lower({{ column_1 }}) is null
    then 'SMB'
    when {{ column_1 }} is not null
    then {{ column_1 }}
end

{%- endmacro -%}
