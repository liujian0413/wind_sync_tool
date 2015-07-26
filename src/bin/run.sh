#!/bin/bash
HM=`dirname $0`/..

$HM/ftp2gz.sh  
$HM/gz2xml.sh
$HM/merge.sh
