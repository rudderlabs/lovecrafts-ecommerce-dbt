with cte_id_stitched_product_added as (

    select distinct b.{{ var('main_id') }}, 
    properties_{{ var('product_ref_var') }}, 
    {{ var('col_ecommerce_product_added_timestamp') }} 
    from {{ var('tbl_ecommerce_product_added') }} a 
    left join {{ var('tbl_id_stitcher')}} b 
    on (a.{{ var('col_ecommerce_product_added_user_id')}} = b.{{ var('col_id_stitcher_other_id')}} and b.{{ var('col_id_stitcher_other_id_type')}} = 'user_id')

), cte_products_added_in_past_n_days as (
    {% for lookback_days in var('lookback_days') %}
        select {{ var('main_id') }},
        array_agg(distinct cast(properties_{{ var('product_ref_var') }} as string)) as products_added_in_past_n_days,
        {{lookback_days}} as n_value
        from cte_id_stitched_product_added
        where datediff(day, date({{ var('col_ecommerce_product_added_timestamp') }}), date({{get_end_timestamp()}})) <= {{lookback_days}} and {{timebound( var('col_ecommerce_product_added_timestamp'))}} and {{ var('main_id')}} is not null
        group by {{ var('main_id') }}
        {% if not loop.last %} union {% endif %}
    {% endfor %}
)

{% for lookback_days in var('lookback_days') %}
select 
    {{ var('main_id') }}, 
    'products_added_in_past_{{lookback_days}}_days' as feature_name, products_added_in_past_n_days as feature_value 
from cte_products_added_in_past_n_days where n_value = {{lookback_days}}
{% if not loop.last %} union {% endif %}
{% endfor %}