use dwd;
insert overwrite table product 
select product_code,
       product_name,
       color_code,
       color_name
from ods.product
;
