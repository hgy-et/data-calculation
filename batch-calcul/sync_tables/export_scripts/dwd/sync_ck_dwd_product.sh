#!/usr/bin/env bash
set -euo pipefail
# set -x
start_datetime=${1}
end_datetime=${2}
backtrack_day_num=${3}
topic_hour=${4}

echo "执行导出前SQL(建表、删数据)："

sqoop eval \
--driver com.github.housepower.jdbc.ClickHouseDriver \
--connect 'jdbc:clickhouse://localhost:8123/system' \
--username user_admin --password '' \
--query "CREATE DATABASE IF NOT EXISTS dwd"

sqoop eval \
--driver com.github.housepower.jdbc.ClickHouseDriver \
--connect 'jdbc:clickhouse://localhost:8123/system' \
--username user_admin --password '' \
--query "DROP TABLE IF EXISTS dwd.product"

sqoop eval \
--driver com.github.housepower.jdbc.ClickHouseDriver \
--connect 'jdbc:clickhouse://localhost:8123/system' \
--username user_admin --password '' \
--query "CREATE TABLE IF NOT EXISTS dwd.product
(
    product_code String comment 'SKC',
    product_name String comment '商品名称',
    color_code String comment '颜色编码',
    color_name String comment '颜色'
)  ENGINE = MergeTree
            ORDER BY (product_code)"

echo "生成sea_tunnel配置文件:"
echo "
env {
  spark.app.name = \"export-dwd.product\"
  spark.executor.instances = 2
  spark.executor.cores = 2
  spark.executor.memory = \"2g\"
  
  spark.sql.catalogImplementation = \"hive\"
}
source {
  hive {
    pre_sql = \"select product_code,product_name,color_code,color_name"\
    result_table_name = \"export_dwd_product\"
  }

}
transform {

}
sink {
  clickhouse {
    host = \"loclahost:8123\"
    clickhouse.socket_timeout = 60000
    database = \"dwd\"
    table = \"product\"
    fields = ["product_code", "product_name", "color_code", "color_name"]
    username = \"user_admin\"
    password = \"\"
  }
}
" > dwd.product.conf

work_dir=$(pwd)

echo "导出数据:"
if ! (cd ${sea_tunnel_dir} && sh ${sea_tunnel_path} -e cluster -m yarn -c "${work_dir}/dwd.product.conf"); then exit 1; fi


