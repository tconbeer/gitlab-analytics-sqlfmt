{%- macro bamboohr_department_grouping(department) -%}

case
    when
        {{ department }}
        in ('Awareness', 'Communications', 'Community Relations', 'Owned Events')
    then 'Awareness, Communications, Community Relations, Owned Events'
    when
        {{ department }}
        in ('Brand & Digital Design', 'Content Marketing', 'Inbound Marketing')
    then 'Brand & Digital Design, Content Marketing, Inbound Marketing'
    when {{ department }} in ('Campaigns', 'Digital Marketing', 'Partner Marketing')
    then 'Campaigns, Digital Marketing, Partner Marketing'
    when
        {{ department }}
        in (
            'Consulting Delivery',
            'Customer Success',
            'Education Delivery',
            'Practice Management'
        )
    then
        'Consulting Delivery, Customer Success, Education Delivery, Practice Management'
    when {{ department }} in ('Field Marketing', 'Marketing Ops')
    then 'Field Marketing, Marketing Ops'
    when {{ department }} in ('People Success', 'CEO')
    then 'People Success, CEO'
    when {{ department }} in ('Product Management', 'Product Strategy')
    then 'Product Management, Product Strategy'
    when {{ department }} in ('Field Ops - Child', 'Field Operations')
    then 'Field Ops - Child, Field Operations'
    else {{ department }}
end

{%- endmacro -%}
