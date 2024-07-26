with
    transaction_lines as (select * from {{ ref("netsuite_transaction_lines") }}),
    transaction_lines_pii as (

        select * from {{ ref("netsuite_transaction_lines_pii") }}

    ),
    transactions as (select * from {{ ref("netsuite_transactions") }}),
    accounts as (select * from {{ ref("netsuite_accounts") }}),
    subsidiaries as (select * from {{ ref("netsuite_subsidiaries") }}),
    subsidiaries_pii as (select * from {{ ref("netsuite_subsidiaries_pii") }}),
    entity as (select * from {{ ref("netsuite_entity") }}),
    entity_pii as (select * from {{ ref("netsuite_entity_pii") }})

select
    tl.transaction_lines_unique_id,
    tl.transaction_id,
    tl.transaction_line_id,
    tl.account_id,
    tl.class_id,
    tl.department_id,
    tl.subsidiary_id,
    tl.receipt_url,
    tl.amount,
    tl.gross_amount,
    a.account_name,
    a.account_full_name,
    a.account_full_description,
    a.account_number,
    a.account_type,
    case
        when lower(a.account_name) like '%contract%'
        then substring(s.subsidiary_name_hash, 16)
        else s_pii.subsidiary_name
    end as subsidiary_name,
    case
        when lower(a.account_name) like '%contract%'
        then substring(tl.memo_hash, 16)
        else t_pii.memo
    end as memo,
    case
        when lower(a.account_name) like '%contract%'
        then substring(e.entity_name_hash, 16)
        when t.entity_id is not null
        then e2.entity_name
        else e_pii.entity_name
    end as entity_name
from transaction_lines tl
left join
    transaction_lines_pii t_pii
    on t_pii.transaction_lines_unique_id = tl.transaction_lines_unique_id
left join transactions t on tl.transaction_id = t.transaction_id
left join entity e on tl.company_id = e.entity_id
left join entity_pii e_pii on e_pii.entity_id = e.entity_id
left join entity_pii e2 on t.entity_id = e2.entity_id
left join accounts a on tl.account_id = a.account_id
left join subsidiaries s on tl.subsidiary_id = s.subsidiary_id
left join subsidiaries_pii s_pii on s_pii.subsidiary_id = s.subsidiary_id
