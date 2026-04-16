-- mart_monthly_sighting_trends.sql
-- How do grackle abundance and flock sizes shift over time?

with base as (
    select * from {{ ref('fct_observations') }}
),

monthly as (
    select
        year,
        month,
        format_date('%B', date(year, month, 1)) as month_name,  
        data_source,
        count(distinct sighting_id)      as unique_sightings,
        count(*)                         as total_records,
        sum(individual_count)            as total_individuals,
        round(avg(individual_count), 2)  as avg_flock_size,
        count(distinct state)            as states_covered
    from base
    group by 1, 2, 3, 4
)

select * from monthly
order by year desc, month desc