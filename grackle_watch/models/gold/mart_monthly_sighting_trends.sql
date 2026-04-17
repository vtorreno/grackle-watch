-- mart_monthly_sighting_trends.sql
-- How do grackle abundance and flock sizes shift over time?

with base as (
    select * from {{ ref('fct_observations') }}
),

monthly as (
    select
        year,
        month,
        date(year, month, 1)                     as month_date,
        format_date('%B', date(year, month, 1))  as month_name,
        format_date('%b %Y', date(year, month, 1)) as month_label,
        data_source,
        count(distinct sighting_id)              as unique_sightings,
        count(*)                                 as total_records,
        sum(individual_count)                    as total_individuals,
        round(avg(individual_count), 2)          as avg_flock_size,
        count(distinct state)                    as states_covered
    from base
    group by 1, 2, 3, 4, 5, 6
)

select * from monthly
order by year desc, month desc