CREATE DATABASE IF NOT EXISTS dwd;
DROP TABLE IF EXISTS dwd.product;
CREATE TABLE IF NOT EXISTS dwd.product
(
    product_code String comment '商品编码',
    product_name String comment '商品名称',
    color_code String comment '颜色代码',
    color_name String comment '颜色名称'
)  ENGINE = MergeTree
            ORDER BY (product_code);
            
