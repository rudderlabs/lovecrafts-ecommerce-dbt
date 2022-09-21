with cte_id_stitched_identifies as (

    {{id_stitch( var('tbl_ecommerce_identifies'), [ var('col_ecommerce_identifies_user_id') , var('col_ecommerce_identifies_email')])}}
)

select * from cte_id_stitched_identifies