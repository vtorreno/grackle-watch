-- mart_subject_learning_curves.sql
-- How quickly did each subject learn across trials?
-- Tracks rolling success rate per bird across water tube and color test experiments

with water_tube as (
    select
        bird,
        trial,
        experiment,
        choice_correct as is_correct,
        'water_tube' as experiment_type
    from {{ ref('stg_knb_water_tube') }}
),

color_test as (
    select
        bird,
        trial,
        experiment,
        is_correct,
        'color_test' as experiment_type
    from {{ ref('stg_knb_color_test') }}
),

combined as (
    select * from water_tube
    union all
    select * from color_test
),

aggregated as (
    select
        bird,
        experiment_type,
        experiment,
        trial,
        count(*)                                                  as total_choices,
        sum(case when is_correct then 1 else 0 end)               as correct_choices,
        round(avg(case when is_correct then 1.0 else 0.0 end), 4) as success_rate
    from combined
    group by bird, experiment_type, experiment, trial
)

select
    bird,
    experiment_type,
    experiment,
    trial,
    total_choices,
    correct_choices,
    success_rate,
    round(
        least(
            avg(success_rate) over (
                partition by bird, experiment_type
                order by trial
                rows between unbounded preceding and current row
            ), 1.0
        ), 4
    ) as cumulative_success_rate
from aggregated
order by bird, experiment_type, trial