#!/bin/bash
#!/usr/bin/env bash
source ~/.bashrc

#set -x
mvn clean package -Dmaven.test.skip=true

cur_script_dir=$(cd "$(dirname "$0")";pwd)

cur_ori_script_dir=$(dirname $(readlink -f "$0"))

work_dir=${cur_script_dir}
project_dir_name=${work_dir##*/}
project_name=${project_dir_name#source-}

shopt -s extglob
cd ${cur_script_dir}/target && rm -rf !(.jar|jar) && cd ../
shopt -u extglob

environment_type=$1
worker_name=$2
auto_gen=${3:-0}

echo environment_type="$1" worker_name="$2"

#args check
if [[ -z $1 ]] || { [[ ! "/$environ_types/" =~ /$1/ ]] && [[ "$1" != "all" ]]; }; then
  echo 'Invalid env_platform_type: ' "$1"
  Usage
  exit 1
fi

if [[ -z $2 ]]; then
  Usage
  exit 1
fi

set -euo pipefail

PackageCore() {
  env_platform_type=$1
  platform='customer'
  if [[ ${env_platform_type} =~ test ]]; then
    platform='test'
  fi


  source "${cur_ori_script_dir}"/conf/default_conf.sh
  source "${cur_script_dir}"/conf/conf.sh

  job_retry_num=${job_retry_num}
  job_retry_interval=${job_retry_interval}
  suffix=''
  if [[ ${env_platform_type} =~ "sandbox" ]]; then
    suffix='_sandbox'
#    job_retry_num=0
#    job_retry_interval=0
  fi


  failure_emails=$(eval echo '$'"failure_emails_${platform}")
  hive_default_data_dir=$(eval echo '$'"hive_default_data_dir_${platform}")

  azkaban_url=$(eval echo '$'"azkaban_url_${platform}")
  azkaban_user=$(eval echo '$'"azkaban_user_${platform}")
  azkaban_password=$(eval echo '$'"azkaban_password_${platform}")

  echo """# system.properties
# this properties can be replaced from azkaban ui(flow parameters).

table_name=\"undefined\"
topic_hour=\"\${azkaban.flow.start.year}\${azkaban.flow.start.month}\${azkaban.flow.start.day}\${azkaban.flow.start.hour}\"
start_datetime=\"\${azkaban.flow.start.year}-\${azkaban.flow.start.month}-\${azkaban.flow.start.day}\"
end_datetime=\"\${azkaban.flow.start.year}-\${azkaban.flow.start.month}-\${azkaban.flow.start.day}\"
backtrack_day_num=0
overall_subtract_day=0

env_type=prod
env_platform_type=${env_platform_type}

engine=spark
hive_max_dynamic_partitions=12000
hive_max_dynamic_partitions_pernode=4000

failure.emails=${failure_emails}
alert.type=sms

retry_interval=${retry_interval}
max_wait_time=${max_wait_time}

# 失败后 间隔指定时间 失败后重跑至多指定次数
retries=${job_retry_num}
retry.backoff=${job_retry_interval}

user.to.proxy=azkaban_hive
submit_user=\${azkaban.flow.submituser}
""" >"${cur_script_dir}"/src/system.properties

  dest_file_name=common_param.sh

  echo "" "#!/usr/bin/env bash
project_name=${project_name}
env_platform_type=${env_platform_type}
env_suffix=${suffix}
temp_db=${temp_db}${suffix}
meta_db=${meta_db}${suffix}
dwd_db=${dwd_db}${suffix}
dws_db=${dws_db}${suffix}
ads_db=${ads_db}${suffix}

default_map_num=${default_map_num}
hive_default_data_dir=${hive_default_data_dir}

# config for tables to update
update_main_tables=${update_main_tables}
update_daily_tables=${update_daily_tables}
disable_auto_gen_update_job=${disable_auto_gen_update_job}
realtime_tables=${realtime_tables}

wait_check_jobs=${wait_check_jobs}
disable_auto_gen_wait_check_jobs=${disable_auto_gen_wait_check_jobs}

azkaban_url=${azkaban_url}
azkaban_user=${azkaban_user}
azkaban_password=${azkaban_password}
" "" >"${cur_script_dir}"/src/common_param.sh

  if [[ ${auto_gen} -eq 1 ]]; then
    echo " >>>>>> generate fetch data job scripts >>>>>> "
    if ! sh "${cur_script_dir}"/src/data-collection/data_fetch_outer_source/fetch_data_module_generator.sh "${env_platform_type}"; then
      exit 1
    fi

    echo "generate update tables job scripts"
    if ! sh "${cur_script_dir}"/src/data-collection/data_update/data_update_jobs/update_data_job_generator.sh; then
      exit 1
    fi

    echo "generate wait flow succeed job scripts"
    if ! sh "${cur_script_dir}"/src/wait_flow_succeed_jobs/wait_job_generator.sh "${suffix}" ; then
      exit 1
    fi
  fi

  zip_file_name=${project_name}-${env_platform_type}_${worker_name}.zip
  if [[ -f ${zip_file_name} ]]; then
    rm "${zip_file_name}"
  fi

  zip_exclude_arg_array=(\"*.DS_Store\" \"*.idea*\" )

  if [[ ! ${suffix} == "_sandbox" ]]; then
    zip_exclude_arg_array+=(\"*sandbox*\")
  fi

  zip_option_arr=(-r ${zip_file_name} "src")
  zip_option_arr+=(-r ${zip_file_name} "flow")
  zip_option_arr+=(-r ${zip_file_name} "target")
  for exclude_str in ${zip_exclude_arg_array[*]}; do
    zip_option_arr+=(--exclude="${exclude_str}")
  done

  cd "${cur_script_dir}"
  zip "${zip_option_arr[@]//\"/}" >/dev/null

  echo Finish package: "${zip_file_name}"

  cd -
  #	rm ${dest_file_name}
  #	mv ${backup_file_name} ${dest_file_name}
}

PackageCore "${environment_type}"
