#!/bin/bash
HM=`dirname $0`/..
source $HM/conf/access.conf

# 将ftp数据同步到本地
$HM/script/dump_4stk_info.pl -O ftp2gz -H "$HM" -D "$DATA_HM" -h $THE_HOST -u $THE_USER -p $THE_PSWD
