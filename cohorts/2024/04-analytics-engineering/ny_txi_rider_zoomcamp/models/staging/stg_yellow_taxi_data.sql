
{{ config(materialized='table') }}
with

source as (
    select *,
    row_number() over (partition by "VendorID", tpep_pickup_datetime) as rnk
     from {{ source('staging', 'yellow_taxi_data') }} 
    WHERE "VendorID" is not null 
    LIMIT 10  -- Limit 100 only records
),

renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['"VendorID"', 'tpep_pickup_datetime'])}} as trip_id,
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee"

        FROM source
)

select * from renamed