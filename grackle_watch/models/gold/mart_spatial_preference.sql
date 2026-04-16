-- mart_spatial_preference.sql
-- Do grackles show consistent spatial bias in their approach direction?
-- Compares each subject's approach rate against random chance to detect behavioral bias.

with base as (
    select * from {{ ref('stg_knb_interaction') }}
),

approach_counts as (
    select
        bird,
        approached_first,
        count(*) as approach_count
    from base
    where approached_first is not null
    group by 1, 2
),

bird_stats as (
    select
        bird,
        sum(approach_count)                     as total_bird_interactions,
        count(distinct approached_first)         as unique_directions_used,
        -- expected rate if choices were purely random
        1.0 / count(distinct approached_first)  as random_chance_threshold
    from approach_counts
    group by 1
),

final as (
    select
        a.bird,
        a.approached_first                                          as approach_direction,
        a.approach_count,
        s.total_bird_interactions,
        s.unique_directions_used,
        round(a.approach_count / s.total_bird_interactions, 4)      as approach_rate,
        round(s.random_chance_threshold, 4)                         as random_chance_threshold,

        -- is this the bird's most frequently chosen direction?
        case
            when a.approach_count = max(a.approach_count) over (partition by a.bird)
            then true
            else false
        end                                                         as is_primary_bias,

        -- is this direction chosen more than 50% above random chance?
        case
            when (a.approach_count / s.total_bird_interactions) > (s.random_chance_threshold * 1.5)
            then true
            else false
        end                                                         as has_significant_preference

    from approach_counts a
    join bird_stats s on a.bird = s.bird
)

select * from final
order by bird, approach_count desc