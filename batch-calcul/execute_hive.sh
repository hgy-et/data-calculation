
set -euo pipefail

function usage() {
    echo  'help'
}

ARGS=`${bin_dir}/getopt -a -o h -l env_type:,sql_file_path:,engine:,hive_max_dynamic_partitions:,hive_max_dynamic_partitions_pernode:,start_datetime:,end_datetime:,backtrack_day_num:,overall_subtract_day:,topic_hour:,is_realtime:,in_sales_season_stat:,past_sales_season_stat:,sales_season:,data_version:,financial_year:,help -- "$@"`
# 重新排列参数的顺序
# 使用eval 的目的是为了防止参数中有shell命令，被错误的扩展。
eval set -- "${ARGS}"


while true
do
      case "$1" in
      --env_type)
              env_type="$2"
              shift
              ;;
              
      --sql_file_path)
              sql_file_path="$2"
              shift
              ;;

       --engine)
              engine="$2"
              shift
              ;;

       --hive_max_dynamic_partitions)
              hive_max_dynamic_partitions="$2"
              shift
              ;;

      --hive_max_dynamic_partitions_pernode)
              hive_max_dynamic_partitions_pernode="$2"
              shift
              ;;

       --start_datetime)
              start_datetime="$2"
              shift
              ;;

      --end_datetime)
              end_datetime="$2"
              shift
              ;;

      --backtrack_day_num)
              backtrack_day_num="$2"
              shift
              ;;

      --overall_subtract_day)
              overall_subtract_day="$2"
              shift
              ;;

      --topic_hour)
              topic_hour="$2"
              shift
              ;;

      --is_realtime)
              is_realtime="$2"
              shift
              ;;

      --in_sales_season_stat)
              in_sales_season_stat="$2"
              shift
              ;;

      --past_sales_season_stat)
              past_sales_season_stat="$2"
              shift
              ;;

      --sales_season)
              sales_season="$2"
              shift
              ;;

      --data_version)
              data_version="$2"
              shift
              ;;

      --financial_year)
              financial_year="$2"
              shift
              ;;
      -h|--help)
              usage
              ;;
      --)
              shift
              break
              ;;
      esac
shift
done

GenerateDates() {
  is_realtime=${1:-0}
  start_date=$(date -d "${start_datetime}" "+%Y-%m-%d")
  end_date=$(date -d "${end_datetime}" "+%Y-%m-%d")
  origin_end_date=${end_date}

  # 非实时的天级别数据抽取,终止日期往前一天
  if [[ ${is_realtime} -eq 0 ]]; then
      # 定时任务使用的是当前时间,而统计数据默认统计昨天及之前的,所以终止日期统一往前移一天
      end_date=$(date -d "${end_date} -1 day" +"%Y-%m-%d")

      # 起始日期不大于终止日期
      if [[ "${start_date}" > "${end_date}" ]]; then
        start_date=$(date -d "${start_date} -1 day" +"%Y-%m-%d")
      fi
  fi

  if [[ ${backtrack_day_num:-0} -gt 0 ]]; then
    start_date=$(date -d "${start_date} -${backtrack_day_num} day" "+%Y-%m-%d")
  fi

  if [[ ${overall_subtract_day:-0} -gt 0 ]]; then
    start_date=$(date -d "${start_date} -${overall_subtract_day} day" "+%Y-%m-%d")
    end_date=$(date -d "${end_date} -${overall_subtract_day} day" "+%Y-%m-%d")
  fi

  start_year=$(date -d "${start_date}" +"%Y") && echo "start_year:" "${start_year}"
  start_year_month=$(date -d "${start_date}" +"%Y-%m") && echo "start_year_month:" "${start_year_month}"
  start_ymd=$(date -d "${start_date}" +"%Y%m%d") && echo "start_ymd:" "${start_ymd}"
  start_year_week=$(date -d "${start_date}" +"%G%V") && echo "start_year_week:" "${start_year_week}"
  start_weekly_1st_date=$(date -d "${start_date} -$(date -d "${start_date}" +"-%u days +1 days")" +"%Y-%m-%d") && echo "start_weekly_1st_date:" "${start_weekly_1st_date}"
  prev_start_date=$(date -d "${start_date} -1day" +"%Y-%m-%d") && echo "prev_start_date:" "${prev_start_date}"
  start_prev_year_month=$(date -d "${start_date} -1month" +"%Y-%m") && echo "start_prev_year_month:" "${start_prev_year_month}"

  end_year=$(date -d "${end_date}" +"%Y") && echo "end_year:" "${end_year}"
  end_year_month=$(date -d "${end_date}" +"%Y-%m") && echo "end_year_month:" "${end_year_month}"
  end_ymd=$(date -d "${end_date}" +"%Y%m%d") && echo "end_ymd:" "${end_ymd}"
  end_year_week=$(date -d "${end_date}" +"%G%V") && echo "end_year_week:" "${end_year_week}"
  end_weekly_1st_date=`date -d "${end_date} +$(date -d "${end_date}" +"-%u days +1 days")" +"%Y-%m-%d"` && echo "end_weekly_last_date:" ${end_weekly_1st_date}
  end_weekly_last_date=$(date -d "${end_date} +$(date -d "${end_date}" +"-%u days +7 days")" +"%Y-%m-%d") && echo "end_weekly_last_date:" "${end_weekly_last_date}"
  end_prev_year_month=$(date -d "${end_date} -1month" +"%Y-%m") && echo "end_prev_year_month:" "${end_prev_year_month}"


  # >>>>>>>> get weekly first days in date range >>>>>>>>
  week_1st_day_array=()
  
  cur_week_1st_day=${start_weekly_1st_date}
  while [[ ! ${cur_week_1st_day} > ${end_weekly_1st_date} ]]
  do
      week_1st_day_array+=("'${cur_week_1st_day}'")
      cur_week_1st_day=`date -d "${cur_week_1st_day} +1 week" +"%Y-%m-%d"`
  done
  
  # >>>>>>>> get monthly last days in date range(end_date as last month's last day) >>>>>>>>
  month_last_day_array[0]="${end_date}"
  cur_month_1st_day=$(date -d "${end_date}" +"%Y-%m-01") && echo "cur_month_1st_day:" "${cur_month_1st_day}"
  prev_month_last_day=$(date -d "${cur_month_1st_day} -1 day" +"%Y-%m-%d") && echo "prev_month_last_day:" "${prev_month_last_day}"

  while [[ ! ${prev_month_last_day} < ${start_date} ]]; do
    month_last_day_array+=("${prev_month_last_day}")

    cur_month_1st_day=$(date -d "${prev_month_last_day}" +"%Y-%m-01") && echo "cur_month_1st_day:" "${cur_month_1st_day}"
    prev_month_last_day=$(date -d "${cur_month_1st_day} -1 day" +"%Y-%m-%d") && echo "prev_month_last_day:" "${prev_month_last_day}"
  done

  function join_by() {
    local IFS="$1"
    shift
    echo "$*"
  }
  
  weekly_1st_days=`join_by , "${week_1st_day_array[@]}"` && echo "weekly_1st_days:" ${weekly_1st_days}
  monthly_last_days=$(join_by , "${month_last_day_array[@]}") && echo "monthly_last_days:" "${monthly_last_days}"
}

GenerateDates ${is_realtime:-0}

temp_db="${db_prefix}temp${env_suffix}"
dwd_db="${db_prefix}dwd${env_suffix}"
dws_db="${db_prefix}dws${env_suffix}"
ads_db="${db_prefix}ads${env_suffix}"

sql_file_name=$(basename ${sql_file_path} .sql) # SQL文件名,作为任务名
hive_job_name=${sql_file_name}

hive_job_name="${db_prefix}${hive_job_name}${env_suffix}"

if [[ ${#hive_job_name} -lt 3  ]]; then
    hive_job_name="${hive_job_name}.hive"
fi

if ! hive --hiveconf mapred.job.name="${hive_job_name}" \
  -hivevar engine="${engine}" \
  -hivevar db_prefix="${db_prefix}" \
  -hivevar env_suffix="${env_suffix}" \
  -hivevar temp_db="${temp_db}" \
  -hivevar dwd_db="${dwd_db}" \
  -hivevar dws_db="${dws_db}" \
  -hivevar ads_db="${ads_db}" \
  \
  -hivevar max_dynamic_partitions="${hive_max_dynamic_partitions}" \
  -hivevar max_dynamic_partitions_pernode="${hive_max_dynamic_partitions_pernode}" \
  \
  -hivevar start_topic_year="${start_year}" \
  -hivevar start_topic_date="${start_date}" \
  -hivevar start_ymd="${start_ymd}" \
  -hivevar start_year_week="${start_year_week}" \
  -hivevar start_weekly_1st_date="${start_weekly_1st_date}" \
  -hivevar prev_start_y_m_d="${prev_start_date}" \
  -hivevar topic_hour="${topic_hour}" \
  -hivevar in_sales_season_start_date="${in_sales_season_start_date:-}" \
  -hivevar in_sales_season_end_date="${in_sales_season_end_date:-}" \
  -hivevar in_sales_seasons="${in_sales_seasons:-NULL}" \
  -hivevar past_sales_season_start_date="${past_sales_season_start_date:-}" \
  -hivevar past_sales_season_end_date="${past_sales_season_end_date:-}" \
  -hivevar past_sales_seasons="${past_sales_seasons:-NULL}" \
  \
  -hivevar sales_season="${sales_season:-}" \
  -hivevar data_version="${data_version:-}" \
  -hivevar financial_year="${financial_year:-}" \
  \
  -hivevar end_topic_year="${end_year}" \
  -hivevar end_topic_date="${end_date}" \
  -hivevar end_ymd="${end_ymd}" \
  -hivevar end_year_week="${end_year_week}" \
  -hivevar end_weekly_1st_date="${end_weekly_1st_date}" \
  -hivevar end_weekly_last_date="${end_weekly_last_date}" \
  -hivevar monthly_last_days="${monthly_last_days}" \
  -hivevar weekly_1st_days="${weekly_1st_days}" \
  \
  -f ${sql_file_path} -v;
then exit 1; fi
