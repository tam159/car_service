select date, sum(cast(revenue as numeric)) as revenue, "subscription" as revenue_type
from {{ ref("vw_subscription_events") }}
where event_type <> "subscription_cancelled"
group by date

union all

select date(timestamp) as date, sum(cast(revenue as numeric)) as revenue, "hardware" as revenue_type
from {{ source("raw_car", "hardware_sales") }}
group by date
