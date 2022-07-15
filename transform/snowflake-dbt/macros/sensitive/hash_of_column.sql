{% macro hash_of_column(column) %}

sha2(
    trim(
        lower(
            {{ column | lower }} || encrypt_raw(
                to_binary('{{ get_salt(column|lower) }}', 'utf-8'),
                to_binary('{{ env_var("SALT_PASSWORD") }}', 'HEX'),
                to_binary('416C736F4E637265FFFFFFAB', 'HEX')
            ) ['ciphertext'
            ]::varchar
        )
    )
) as {{ column | lower }}_hash,

{% endmacro %}
