with
    source as (

        select * from {{ ref("sheetload_data_team_csat_survey_fy2021_q4_source") }}

    ),
    final as (

        select
            nullif("Timestamp", '')::varchar::timestamp as survey_timestamp,
            nullif("Which_division_are_you_a_part_of?", '')::varchar as division,
            nullif("What_is_your_role_at_GitLab?", '')::varchar as role,
            nullif("Where_are_you_located?", '')::varchar as location,
            nullif(
                "How_do_you_normally_interact_with_the_Data_Team?", ''
            )::varchar as interaction_with_data_team,
            nullif(
                "How_often_do_you_interact_with_the_Data_Team?", ''
            )::varchar as how_often_interaction_with_data_team,
            nullif(
                "What_Data_Team_solutions_do_you_regularly_use?_Please_check_all_that_apply.",
                ''
            )::varchar as data_team_solutions,
            try_to_number(
                "How_important_is_the_Data_Team_to_your_success?_"
            ) as data_team_importance_rating,
            try_to_number(
                "Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Collaboration."
            ) as data_team_collaboration_rating,
            try_to_number(
                "Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Results._"
            ) as data_team_results_rating,
            try_to_number(
                "Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Efficiency."
            ) as data_team_efficiency_rating,
            try_to_number(
                "Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Diversity,_Inclusion,_&_Belonging."
            ) as data_team_diversity_inclusion_belonging_rating,
            try_to_number(
                "Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Iteration."
            ) as data_team_iteration_rating,
            try_to_number(
                "Please_rate_your_experience_with_the_Data_Team_in_the_area_of_Transparency."
            ) as data_team_transparency_rating,
            try_to_number(
                "How_would_you_rate_the_Overall_Quality_of_the_Results_you_received_from_the_Data_Team?"
            ) as data_team_overall_quality_rating,
            nullif(
                "What_is_the_Data_Team_doing_well?_Please_be_specific_as_possible.", ''
            )::varchar as what_data_team_is_doing_well,
            nullif(
                "What_can_the_Data_Team_improve_on?_Please_be_specific_as_possible.", ''
            )::varchar as what_data_team_needs_to_improve
        from source

    )

select *
from final
