WITH dim_accounts AS (

    SELECT *
    FROM {{ ref('dim_accounts') }}

), dim_customers AS (

    SELECT *
    FROM {{ ref('dim_customers') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), dim_product_details AS (

    SELECT *
    FROM {{ ref('dim_product_details') }}

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}

), mart_arr AS (

    SELECT
      dim_dates.date_actual                                                           AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS fiscal_year,
      dim_customers.ultimate_parent_account_name,
      dim_customers.ultimate_parent_account_id,
      dim_product_details.product_category,
      dim_product_details.delivery,
      fct_mrr.mrr,
      fct_mrr.quantity
    FROM fct_mrr
    INNER JOIN dim_subscriptions
      ON dim_subscriptions.subscription_id = fct_mrr.subscription_id
    INNER JOIN dim_product_details
      ON dim_product_details.product_details_id = fct_mrr.product_details_id
    INNER JOIN dim_accounts
      ON dim_accounts.account_id = fct_mrr.account_id
    INNER JOIN dim_dates
      ON dim_dates.date_id = fct_mrr.date_id
    LEFT JOIN dim_customers
      ON dim_accounts.crm_id = dim_customers.crm_id

), max_min_month AS (

    SELECT
      ultimate_parent_account_name,
      ultimate_parent_account_id,
      MIN(arr_month)                      AS date_month_start,
      --add 1 month to generate churn month
      DATEADD('month',1,MAX(arr_month))   AS date_month_end
    FROM mart_arr
    {{ dbt_utils.group_by(n=2) }}

), base AS (

    SELECT
      ultimate_parent_account_name,
      ultimate_parent_account_id,
      dim_dates.date_actual         AS arr_month,
      dim_dates.fiscal_quarter_name_fy,
      dim_dates.fiscal_year
    FROM max_min_month
    INNER JOIN dim_dates
      -- all months after start date
      ON  dim_dates.date_actual >= max_min_month.date_month_start
      -- up to and including end date
      AND dim_dates.date_actual <=  max_min_month.date_month_end
      AND day_of_month = 1

), yearly_arr_parent_level AS (

    SELECT
      base.fiscal_year,
      base.arr_month                                                                         AS arr_year,
      base.ultimate_parent_account_name,
      base.ultimate_parent_account_id,
      ARRAY_AGG(DISTINCT product_category) WITHIN GROUP (ORDER BY product_category ASC)      AS product_category,
      ARRAY_AGG(DISTINCT delivery) WITHIN GROUP (ORDER BY delivery ASC)                      AS delivery,
      MAX(DECODE(product_category,   --Need to account for the 'other' categories
          'Bronze', 1,
          'Silver', 2,
          'Gold', 3,

          'Starter', 1,
          'Premium', 2,
          'Ultimate', 3,
          0
     ))                                                                                       AS product_ranking,
      SUM(ZEROIFNULL(quantity))                                                               AS quantity,
      SUM(ZEROIFNULL(mrr)*12)                                                                 AS arr
    FROM base
    LEFT JOIN mart_arr
      ON base.arr_month = mart_arr.arr_month
      AND base.ultimate_parent_account_id = mart_arr.ultimate_parent_account_id
    INNER JOIN dim_dates
      ON base.arr_month = dim_dates.date_actual
    WHERE base.arr_month = date_trunc('month', last_day_of_fiscal_year)
    {{ dbt_utils.group_by(n=4) }}

), prior_year AS (

    SELECT
      yearly_arr_parent_level.*,
      LAG(product_category) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_year) AS previous_product_category,
      LAG(delivery) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_year) AS previous_delivery,
      COALESCE(LAG(product_ranking) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_year),0) AS previous_product_ranking,
      COALESCE(LAG(quantity) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_year),0) AS previous_quantity,
      COALESCE(LAG(arr) OVER (PARTITION BY ultimate_parent_account_id ORDER BY arr_year),0) AS previous_arr
    FROM yearly_arr_parent_level

), type_of_arr_change AS (

    SELECT
      prior_year.*,
      {{ type_of_arr_change('arr','previous_arr') }}
    FROM prior_year

), reason_for_arr_change_beg AS (

    SELECT
      arr_year,
      ultimate_parent_account_id,
      previous_arr      AS beg_arr,
      previous_quantity AS beg_quantity
    FROM type_of_arr_change

), reason_for_arr_change_seat_change AS (

    SELECT
      arr_year,
      ultimate_parent_account_id,
      {{ reason_for_arr_change_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      {{ reason_for_quantity_change_seat_change('quantity', 'previous_quantity') }}
    FROM type_of_arr_change

), reason_for_arr_change_price_change AS (

    SELECT
      arr_year,
      ultimate_parent_account_id,
      {{ reason_for_arr_change_price_change('product_category', 'previous_product_category', 'quantity', 'previous_quantity', 'arr', 'previous_arr', 'product_ranking',' previous_product_ranking') }}
    FROM type_of_arr_change

), reason_for_arr_change_tier_change AS (

    SELECT
      arr_year,
      ultimate_parent_account_id,
      {{ reason_for_arr_change_tier_change('product_ranking', 'previous_product_ranking', 'quantity', 'previous_quantity', 'arr', 'previous_arr') }}
    FROM type_of_arr_change

), reason_for_arr_change_end AS (

    SELECT
      arr_year,
      ultimate_parent_account_id,
      arr                   AS end_arr,
      quantity              AS end_quantity
    FROM type_of_arr_change

), annual_price_per_seat_change AS (

    SELECT
      arr_year,
      ultimate_parent_account_id,
      {{ annual_price_per_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }}
    FROM type_of_arr_change

), combined AS (

    SELECT
      {{ dbt_utils.surrogate_key(['type_of_arr_change.arr_year', 'type_of_arr_change.ultimate_parent_account_id']) }}
                                                                        AS primary_key,
      type_of_arr_change.fiscal_year,
      type_of_arr_change.arr_year,
      type_of_arr_change.ultimate_parent_account_name,
      type_of_arr_change.ultimate_parent_account_id,
      type_of_arr_change.product_category,
      type_of_arr_change.previous_product_category                      AS previous_year_product_category,
      type_of_arr_change.delivery,
      type_of_arr_change.previous_delivery                              AS previous_year_delivery,
      type_of_arr_change.product_ranking,
      type_of_arr_change.previous_product_ranking                       AS previous_year_product_ranking,
      type_of_arr_change.type_of_arr_change,
      reason_for_arr_change_beg.beg_arr,
      reason_for_arr_change_beg.beg_quantity,
      reason_for_arr_change_seat_change.seat_change_arr,
      reason_for_arr_change_seat_change.seat_change_quantity,
      reason_for_arr_change_price_change.price_change_arr,
      reason_for_arr_change_tier_change.tier_change_arr,
      reason_for_arr_change_end.end_arr,
      reason_for_arr_change_end.end_quantity,
      annual_price_per_seat_change.annual_price_per_seat_change
    FROM type_of_arr_change
    LEFT JOIN reason_for_arr_change_beg
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_beg.ultimate_parent_account_id
      AND type_of_arr_change.arr_year = reason_for_arr_change_beg.arr_year
    LEFT JOIN reason_for_arr_change_seat_change
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_seat_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_year = reason_for_arr_change_seat_change.arr_year
    LEFT JOIN reason_for_arr_change_price_change
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_price_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_year = reason_for_arr_change_price_change.arr_year
    LEFT JOIN reason_for_arr_change_tier_change
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_tier_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_year = reason_for_arr_change_tier_change.arr_year
    LEFT JOIN reason_for_arr_change_end
      ON type_of_arr_change.ultimate_parent_account_id = reason_for_arr_change_end.ultimate_parent_account_id
      AND type_of_arr_change.arr_year = reason_for_arr_change_end.arr_year
    LEFT JOIN annual_price_per_seat_change
      ON type_of_arr_change.ultimate_parent_account_id = annual_price_per_seat_change.ultimate_parent_account_id
      AND type_of_arr_change.arr_year = annual_price_per_seat_change.arr_year
    WHERE type_of_arr_change.arr_year < DATE_TRUNC('month',CURRENT_DATE)

)

SELECT *
FROM combined
