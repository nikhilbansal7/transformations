select
    `__hevo_id` as unique_field,
    count(*) as n_records

from `silver-retina-331504`.`Delivery`.`dlx_delivery`
where `__hevo_id` is not null
group by `__hevo_id`
having count(*) > 1