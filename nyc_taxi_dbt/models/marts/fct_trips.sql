with staging as (
    select * from {{ref('stg_yellow_taxi_h1')}}
),
final as(
    select 
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    vendor_id,
    payment_type,
    case payment_type
    when 1 then 'Credit Card'
    when 2 then 'Cash'
    when 3 then 'No Charge'
    when 4 then 'Dispute'
    when 5 then 'Unknown'
    when 6 then 'Vioded Trip'
    else 'Unknown'
    end as payment_type_label,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    total_revenue,
    is_incomplete_metadata
    from staging
)

select * from final