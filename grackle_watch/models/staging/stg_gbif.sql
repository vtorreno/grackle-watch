-- stg_gbif.sql
with source as (
    select * from {{ source('grackle_watch_external', 'gbif') }}
),

staged as (
    select
        -- identifiers
        cast(gbif_id as string) as sighting_id,   -- cast to string for cross-source UNION compatibility
        occurrence_id,                            -- original source URL for traceability

        -- taxonomy
        scientific_name,
        species,
        iucn_category,

        -- spatial
        latitude,
        longitude,
        st_geogpoint(longitude, latitude) as geo_location,  -- native geography object for spatial queries
        coordinate_uncertainty,
        state,

        -- temporal
        event_date,
        year,
        month,
        day,

        -- biological / behavioral
        cast(count as integer) as individual_count,  -- renamed to avoid SQL reserved word conflict
        sex,
        status,
        record_type,

        -- audit & provenance
        'GBIF' as data_source,           -- hardcoded source label for cross-dataset unions
        source_dataset,                  -- originating GBIF dataset (e.g. iNaturalist)
        observer,
        license,
        current_timestamp() as dbt_staged_at  -- pipeline run timestamp for monitoring

    from source
)

select * from staged