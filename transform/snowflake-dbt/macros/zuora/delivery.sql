{%- macro delivery(product_category_column, output_column_name="delivery") -%}

case
    when
        lower({{ product_category_column }}) like any (
            '%saas%', 'storage', 'standard', 'basic', 'plus', 'githost'
        )
    then 'SaaS'
    when lower({{ product_category_column }}) like '%self-managed%'
    then 'Self-Managed'
    when {{ product_category_column }} in ('Other', 'Support', 'Trueup')
    then 'Others'
    else null
end as {{ output_column_name }}

{%- endmacro -%}
