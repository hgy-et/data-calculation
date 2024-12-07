create table product (
  product_code string comment '商品编码',
  product_name string comment '商品名称',
  color_code string comment '颜色编码',
  color_code string comment '颜色名称'
  )
COMMENT '商品'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\001'
STORED AS TEXTFILE;
