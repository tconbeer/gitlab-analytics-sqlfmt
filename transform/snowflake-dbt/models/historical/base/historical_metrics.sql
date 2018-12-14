WITH source AS (

	SELECT *
	FROM raw.historical.metrics

), renamed AS (


	SELECT uniquekey 												as primary_key,
			month::date 												as month_of,
			total_revenue::decimal 							as total_revenue,
			licensed_users::decimal 						as licensed_users,
			rev_per_user::decimal  							as revenue_per_user,
			com_paid_users::decimal 						as com_paid_users,
			active_core_hosts::decimal 					as active_core_hosts,
			com_availability::decimal 					as com_availability,
			com_response_time::decimal 					as com_response_time,
			com_monthly_active_users::decimal 	as com_monthly_active_users,
			com_projects::decimal 							as com_projects,
			ending_cash::decimal 								as ending_cash,
			ending_loc::decimal 								as ending_loc,
			cash_change::decimal 								as cash_change,
			avg_monthly_burn::decimal 					as avg_monthly_burn,
			days_outstanding::decimal,
			cash_remaining::decimal,
			rep_prod_annualized::decimal 				as rep_prod_annualized,
			cac::decimal 												as cac,
			ltv::decimal 												as ltv,
			ltv_to_cac::decimal,
			cac_ratio::decimal,
			magic_number::decimal,
			sales_efficiency::decimal,
			gross_burn_rate::decimal 						as gross_burn_rate,
			cap_consumption::decimal 						as capital_consumption,
			sclau::decimal 											as sclau,
			csat::decimal 											as csat,
			on_prem_sla::decimal                as on_prem_sla,
			sub_sla::decimal                    as sub_sla

	FROM source

)

SELECT *
FROM renamed
