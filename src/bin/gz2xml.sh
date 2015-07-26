#!/bin/bash
HM=`dirname $0`/..
source $HM/conf/access.conf

$HM/script/dump_4stk_info.pl -O gz2xml -H "$HM" -D "$DATA_HM"
$HM/script/dump_4stk_info.pl -O xml2inc -H "$HM" -D "$DATA_HM"
