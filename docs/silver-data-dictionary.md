# Diccionario de datos Silver

## Datasets por servicio

### `yellow_trips`

Grano: una fila por viaje Yellow válido o con advertencias.

Grupos principales:

- identidad y lineage: `trip_business_id`, `service_type`, `source_file`, `bronze_sha256`, `bronze_execution_id`, `silver_execution_id`;
- tiempo: `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `dropoff_date`, `pickup_hour`, `pickup_day_of_week`, `trip_duration_seconds`;
- ubicación: `pickup_location_id`, `dropoff_location_id`, nombres de zona, borough y service zone;
- operación: `vendor_id`, `passenger_count`, `trip_distance`, `rate_code_id`, `store_and_fwd_flag`;
- finanzas: `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`;
- derivados: `average_speed_mph`, `fare_per_mile`, `tip_percentage`, `revenue_per_minute`, `airport_trip_flag`;
- calidad: arrays, conteos y estado de calidad.

### `green_trips`

Mismo núcleo de Yellow, más:

```text
trip_type
ehail_fee
```

### `fhv_trips`

Grano: una fila por viaje FHV válido o con advertencias.

Campos característicos:

```text
dispatching_base_num
affiliated_base_number
shared_ride_flag
pickup_datetime
dropoff_datetime
pickup_location_id
dropoff_location_id
trip_duration_seconds
```

El enriquecimiento añade los nombres disponibles de bases y zonas.

### `hvfhv_trips`

Grano: una fila por viaje High Volume FHV válido o con advertencias.

Campos característicos:

```text
hvfhs_license_num
hvfhs_company_name
dispatching_base_num
originating_base_num
request_datetime
on_scene_datetime
pickup_datetime
dropoff_datetime
trip_miles
trip_time
base_passenger_fare
tolls
black_car_fund
sales_tax
congestion_surcharge
airport_fee
tips
driver_pay
shared_requested
shared_matched
access_a_ride
wav_requested
wav_matched
request_to_pickup_seconds
driver_wait_seconds
```

## `rejected_records`

Grano: una fila rechazada del archivo Bronze.

Conserva las columnas normalizadas del servicio, lineage completo y las razones exactas del rechazo. Particionamiento:

```text
service_type / year / month
```

## `trips_master`

Grano: una fila por viaje Silver aceptado (`VALID` o `WARNING`).

Campos conformados principales:

### Identidad

```text
service_id
service_type
trip_count
trip_business_id
vendor_id_or_license
```

`service_id`:

```text
1 yellow
2 green
3 fhv
4 fhvhv
```

### Bases y licencias

```text
hvfhs_company_name
dispatching_base_num
dispatching_base_name
dispatching_base_dba
dispatching_base_type
originating_base_num
originating_base_name
affiliated_base_num
affiliated_base_name
```

### Tiempo

```text
request_datetime
on_scene_datetime
pickup_datetime
dropoff_datetime
pickup_date
dropoff_date
pickup_hour
pickup_day_of_week
weekend_trip_flag
night_trip_flag
```

### Geografía

```text
pickup_location_id
pickup_zone_name
pickup_borough
pickup_service_zone
dropoff_location_id
dropoff_zone_name
dropoff_borough
dropoff_service_zone
```

### Operación

```text
passenger_count
trip_distance
trip_duration_seconds
reported_trip_time_seconds
request_to_pickup_seconds
driver_wait_seconds
average_speed_mph
rate_code_id
payment_type
trip_type
```

### Finanzas

```text
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount
congestion_surcharge
airport_fee
cbd_congestion_fee
black_car_fund
sales_tax
driver_pay
fare_per_mile
tip_percentage
revenue_per_minute
```

### Indicadores

```text
store_and_fwd_flag
shared_requested
shared_matched
access_a_ride
wav_requested
wav_matched
airport_trip_flag
```

### Calidad y lineage

```text
quality_status
quality_warning_codes
quality_warning_count
source_file
source_year
source_month
bronze_sha256
bronze_execution_id
silver_execution_id
silver_processed_at
```

Las columnas no aplicables a un servicio son nulas por diseño. Los atributos altamente especializados continúan disponibles en el dataset Silver específico.

## Referencias

### `taxi_zones`

```text
location_id
borough
zone_name
service_zone
is_airport
airport_name
source_url
source_sha256
refreshed_at
```

### `base_lookup`

```text
base_license_number
base_name
doing_business_as
base_type
status
telephone
source_url
source_sha256
refreshed_at
```
