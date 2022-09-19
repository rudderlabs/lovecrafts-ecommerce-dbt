select 
    {{ var('main_id') }}, 
    avg({{ var('col_ecommerce_order_completed_properties_total') }}) as avg_transaction_value 
from {{ ref('order_completed') }}
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
group by {{ var('main_id') }}