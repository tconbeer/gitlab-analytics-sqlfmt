with
    source as (select * from {{ source("zoominfo", "contact_enhance") }}),

    renamed as (

        select
            "Record ID"::varchar as record_id,
            row_integer::varchar as user_id,
            first_name::varchar as first_name,
            last_name::varchar as last_name,
            users_name::varchar as users_name,
            email_id::varchar as email_id,
            internal_value1::varchar as internal_value1,
            internal_value2::varchar as internal_value2,
            company_name::varchar as company_name,
            parent_company_name::varchar as parent_company_name,
            email_type::varchar as email_type,
            "Match status"::varchar as match_status,
            "ZoomInfo Contact ID"::varchar as zoominfo_contact_id,
            "Last Name"::varchar as lastname,
            "First Name"::varchar as firstname,
            "Middle Name"::varchar as middlename,
            "Salutation"::varchar as salutation,  -- noqa:L059
            "Suffix"::varchar as suffix,  -- noqa:L059
            "Job Title"::varchar as job_title,
            "Job Function"::varchar as job_function,
            "Management Level"::varchar as management_level,
            "Company Division Name"::varchar as company_division_name,
            "Direct Phone Number"::varchar as direct_phone_number,
            "Email Address"::varchar as email_address,
            "Email Domain"::varchar as email_domain,
            "Department"::varchar as department,  -- noqa:L059
            "Supplemental Email"::varchar as supplemental_email,
            "Mobile phone"::varchar as mobile_phone,
            "Contact Accuracy Score"::varchar as contact_accuracy_score,
            "Contact Accuracy Grade"::varchar as contact_accuracy_grade,
            "ZoomInfo Contact Profile URL"::varchar as zoominfo_contact_profile_url,
            "LinkedIn Contact Profile URL"::varchar as linkedin_contact_profile_url,
            "Notice Provided Date"::varchar as notice_provided_date,
            "Known First Name"::varchar as known_first_name,
            "Known Last Name"::varchar as known_last_name,
            "Known Full Name"::varchar as known_full_name,
            "Normalized First Name"::varchar as normalized_first_name,
            "Normalized Last Name"::varchar as normalized_last_name,
            "Email Matched Person Name"::varchar as email_matched_person_name,
            "Email Matched Company Name"::varchar as email_matched_company_name,
            "Free Email"::varchar as free_email,
            "Generic Email"::varchar as generic_email,
            "Malformed Email"::varchar as malformed_email,
            "Calculated Job Function"::varchar as calculated_job_function,
            "Calculated Management Level"::varchar as calculated_management_level,
            "Person Has Moved"::varchar as person_has_moved,
            "Person Looks Like EU"::varchar as person_looks_like_eu,
            "Within EU"::varchar as within_eu,
            "Person Street"::varchar as person_street,
            "Person City"::varchar as person_city,
            "Person State"::varchar as person_state,
            "Person Zip Code"::varchar as person_zip_code,
            "Country"::varchar as country,  -- noqa:L059
            "Company Name"::varchar as companyname,
            "Website"::varchar as website,  -- noqa:L059
            "Founded Year"::varchar as founded_year,
            "Company HQ Phone"::varchar as company_hq_phone,
            "Fax"::varchar as fax,  -- noqa:L059
            "Ticker"::varchar as ticker,  -- noqa:L059
            "Revenue (in 000s)"::varchar as revenue,
            "Revenue Range"::varchar as revenue_range,
            "Est. Marketing Department Budget (in 000s)"::varchar
            as est_marketing_department_budget,  -- noqa:L026,L028,L016
            "Est. Finance Department Budget (in 000s)"::varchar
            as est_finance_department_budget,  -- noqa:L026,L016
            -- noqa:L026
            "Est. IT Department Budget (in 000s)"::varchar as est_it_department_budget,
            -- noqa:L026
            "Est. HR Department Budget (in 000s)"::varchar as est_hr_department_budget,
            "Employees"::varchar as employees,  -- noqa:L059
            "Employee Range"::varchar as employee_range,
            "Past 1 Year Employee Growth Rate"::varchar
            as past_1_year_employee_growth_rate,
            "Past 2 Year Employee Growth Rate"::varchar
            as past_2_year_employee_growth_rate,
            "SIC Code 1"::varchar as sic_code_1,
            "SIC Code 2"::varchar as sic_code_2,
            "SIC Codes"::varchar as sic_codes,
            "NAICS Code 1"::varchar as naics_code_1,
            "NAICS Code 2"::varchar as naics_code_2,
            "NAICS Codes"::varchar as naics_codes,
            "Primary Industry"::varchar as primary_industry,
            "Primary Sub-Industry"::varchar as primary_sub_industry,
            "All Industries"::varchar as all_industries,
            "All Sub-Industries"::varchar as all_sub_industries,
            "Industry Hierarchical Category"::varchar as industry_hierarchical_category,
            "Secondary Industry Hierarchical Category"::varchar
            as secondary_industry_hierarchical_category,
            "Alexa Rank"::varchar as alexa_rank,
            "ZoomInfo Company Profile URL"::varchar as zoominfo_company_profile_url,
            "LinkedIn Company Profile URL"::varchar as linkedin_company_profile_url,
            "Facebook Company Profile URL"::varchar as facebook_company_profile_url,
            "Twitter Company Profile URL"::varchar as twitter_company_profile_url,
            "Ownership Type"::varchar as ownership_type,
            "Business Model"::varchar as business_model,
            "Certified Active Company"::varchar as certified_active_company,
            "Certification Date"::varchar as certification_date,
            "Total Funding Amount (in 000s)"::varchar as total_funding_amount,
            "Recent Funding Amount (in 000s)"::varchar as recent_funding_amount,
            "Recent Funding Date"::varchar as recent_funding_date,
            "Company Street Address"::varchar as company_street_address,
            "Company City"::varchar as company_city,
            "Company State"::varchar as company_state,
            "Company Zip Code"::varchar as company_zip_code,
            "Company Country"::varchar as company_country,
            "Full Address"::varchar as full_address,
            "Number of Locations"::varchar as number_of_locations,
            iff(
                "ZoomInfo Company ID" = '' or "ZoomInfo Company ID" = 0,
                null,
                "ZoomInfo Company ID"
            )::varchar as zoominfo_company_id
        from source

    )

select *
from renamed
