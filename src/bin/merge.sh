#!/bin/bash
HM=$(dirname `readlink -f $0`)/..
source $HM/conf/access.conf

cat $HM/parse.conf |egrep "^[a-zA-Z]"| while read SRC;
do
	echo "merging: $SRC"

	#SRC=AShareMoneyFlow
	INC_DAT=$DATA_HM/$SRC/inc

	rm $MERGE_DIR/$SRC.fnl.txt
	find $INC_DAT/*.inc |sort |while read line;
	do
		echo "\t$line"
		cat $line | $HM/script/merge.pl >> $MERGE_DIR/$SRC.fnl.txt
	done

	cd $MERGE_DIR 
		tar zcvf $SRC.fnl.tgz $SRC.fnl.txt
	cd $HM

done
