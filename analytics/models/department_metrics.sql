with latest_valid_appointments as (
    select distinct on (appointment_id)
        appointment_id,
        department,
        status,
        lab_tests
    from {{ source('raw', 'appointments') }}
    order by appointment_id, kafka_timestamp desc, topic desc, kafka_partition desc, kafka_offset desc
)
select
    department,
    count(*) as appointments,
    count(*) filter (where status = 'completed') as completed,
    count(*) filter (where status = 'cancelled') as cancelled,
    count(*) filter (where status = 'scheduled') as scheduled,
    sum(lab_tests) as lab_tests
from latest_valid_appointments
group by department
