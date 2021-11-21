SELECT count(*)
FROM  `silver-retina-331504`.`Delivery`.`dlx_delivery` AS table_1

       INNER JOIN (SELECT `__hevo_id`                      AS key,
                          Max(__hevo__ingested_at) AS latest_ingest
                          --Max(__hevo__loaded_at)   AS latest_load       
                   FROM
                          `silver-retina-331504`.`Delivery`.`dlx_delivery`
                   GROUP  BY `__hevo_id`) AS table_2

               ON table_1.`__hevo_id` = table_2.key
                  AND table_1.__hevo__ingested_at = table_2.latest_ingest
                  --AND table_1.__hevo__loaded_at = table_2.latest_load                  
)
--WHERE  __hevo__marked_deleted = false