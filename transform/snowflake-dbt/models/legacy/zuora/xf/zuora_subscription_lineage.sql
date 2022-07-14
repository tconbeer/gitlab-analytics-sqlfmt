{{ config({"schema": "legacy"}) }}

with recursive

    flattening as (

        select
            {{
                dbt_utils.star(
                    from=ref("zuora_subscription_intermediate"),
                    except=["ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY"],
                )
            }},

            iff(
                array_to_string(zuora_renewal_subscription_name_slugify, ',') is null,
                subscription_name_slugify,
                subscription_name_slugify
                || ',' || array_to_string(
                    zuora_renewal_subscription_name_slugify, ','
                )
            ) as lineage,
            renewal.value::varchar as zuora_renewal_subscription_name_slugify
        from
            {{ ref("zuora_subscription_intermediate") }},
            lateral flatten(
                input => zuora_renewal_subscription_name_slugify, outer => true
            ) renewal
        -- See issue: https://gitlab.com/gitlab-data/analytics/-/issues/6518
        where
            subscription_id not in (
                '2c92a00d6e59c212016e6432a2d70dee',
                '2c92a0ff74357c7401744e2bf3ee614b',
                '2c92a00f74e75dd60174e89220361bbc',
                '2c92a00774ddaf190174de37f0eb147d'
            )
    ),

    zuora_sub(base_slug, renewal_slug, parent_slug, lineage, children_count) as (

        select
            subscription_name_slugify as base_slug,
            zuora_renewal_subscription_name_slugify as renewal_slug,
            subscription_name_slugify as parent_slug,
            lineage as lineage,
            2 as children_count
        from flattening

        union all

        select
            iter.subscription_name_slugify as base_slug,
            iter.zuora_renewal_subscription_name_slugify as renewal_slug,
            anchor.parent_slug as parent_slug,
            anchor.lineage
            || ','
            || iter.zuora_renewal_subscription_name_slugify as lineage,
            iff(
                iter.zuora_renewal_subscription_name_slugify is null,
                0,
                anchor.children_count + 1
            ) as children_count
        from zuora_sub anchor
        join flattening iter on anchor.renewal_slug = iter.subscription_name_slugify

    ),

    pull_full_lineage as (

        select
            parent_slug,
            base_slug,
            renewal_slug,
            first_value(lineage) over (
                partition by parent_slug order by children_count desc
            ) as lineage,
            children_count
        from zuora_sub

    ),

    deduped as (

        select parent_slug as subscription_name_slugify, lineage
        from pull_full_lineage
        group by 1, 2

    )

select *
from deduped
