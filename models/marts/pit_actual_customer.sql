WITH actual_customer as (
    SELECT hc.CUSTOMER_PK as customer_pk,
    cd.first_name,
    cd.last_name,
    cd.email,
    cd.EFFECTIVE_FROM as cd_effective_from,
    coalesce(lead(cd.EFFECTIVE_FROM)over(partition by hc.CUSTOMER_PK order by cd.EFFECTIVE_FROM), '9999-12-31') as cd_effective_to
    FROM {{ref("hub_customer")}} hc
        left join {{ref("sat_customer_details")}} cd ON hc.CUSTOMER_PK=cd.CUSTOMER_PK
)

SELECT  customer_pk,
        first_name,
        last_name,
        email,
        cd_effective_from,
        cd_effective_to FROM actual_customer