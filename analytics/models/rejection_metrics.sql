select rejection_reason, count(*) as rejected_events
from {{ source('raw', 'rejected_events') }}
group by rejection_reason
