with cte_id_stitched_product_added as 
(select distinct b.main_id as main_id, properties_cart_id, timestamp from {{ source('ecommerce', 'product_added') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id')),

cte_carts_in_past_n_days as
({% for lookback_days in var('lookback_days') %}
select main_id,
count(distinct properties_cart_id) as carts_in_past_n_days,
{{lookback_days}} as n_value
from cte_id_stitched_product_added
where datediff(day, date(timestamp), date({{get_end_timestamp()}})) <= {{lookback_days}}
and timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id
{% if not loop.last %} union {% endif %}
{% endfor %})

{% for lookback_days in var('lookback_days') %}
select main_id, 
'carts_in_past_{{lookback_days}}_days' as feature_name, carts_in_past_n_days as feature_value from 
cte_carts_in_past_n_days where n_value = {{lookback_days}}
{% if not loop.last %} union {% endif %}
{% endfor %}