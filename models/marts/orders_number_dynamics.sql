WITH orders_dynamics as (
    select date_trunc('week', sod.order_date) as week_start,
    coalesce(sod.status,'not_specified') as order_status,
    count(*) as number_of_orders
    from {{ref("hub_customer")}} hc 
    left join {{ref("sat_customer_details")}} scd on hc.customer_pk = scd.customer_pk 
    join {{ref("link_customer_order")}} lco on hc.customer_pk = lco.customer_pk 
    left join {{ref("sat_order_details")}} sod on lco.order_pk = sod.order_pk 
    group by date_trunc('week', sod.order_date), sod.status
)

select * from orders_dynamics