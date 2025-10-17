{%- macro pad_column(column, string) -%}

    insert(
        insert({{ column }}, 1, 0, '{{string}}'), len({{ column }}) + 2, 0, '{{string}}'
    )

{%- endmacro -%}
