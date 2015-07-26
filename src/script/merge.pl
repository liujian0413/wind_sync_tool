#!/usr/bin/perl


while(<STDIN>)
{
	chomp;
#002560.SZ^C20150703     1435908091      BUY_TRADES_EXLARGE_ORDER^B8^ABUY_T
	my ($obj_ds,$tck,$info)=split "\t",$_;

	my ($obj,$ds)=split /\x03/,$obj_ds;

	print join("\t",($obj,$ds,$info)),"\n";

}
