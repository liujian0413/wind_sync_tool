#!/usr/bin/perl -w
#############################################################

my $HM=".";
my $DATA_HM="./data_res";

#############################################################

use lib "./module";
use wind_db;
use File::Path;
use strict;
#############################################################

my $OP_TP="ftp2gz";
my $HOST="";
my $USER="";
my $PSWD="";

use Getopt::Long;
Getopt::Long::GetOptions(
	'O=s' => \$OP_TP,
	'H=s' => \$HM,
	'D=s' => \$DATA_HM
	'h=s' => \$HOST,
	'u=s' => \$USER,
	'p=s' => \$PSWD
);
#############################################################

my %conf=();
load_conf(\%conf,"$HM/conf/xml.conf");
my %parse_conf=();
load_parse_conf(\%parse_conf,"$HM/conf/parse.conf");

#
#
#
if("ftp2gz" eq $OP_TP)
{
	foreach my $a_src(sort {$a cmp $b}keys %conf)
	{
		my $LOCAL_HM="$DATA_HM/$a_src";
		my $THE_GZ_DIR="$LOCAL_HM/gz";
		mkpath( $THE_GZ_DIR);

		my @new_files=();
		foreach my $a_val(sort {$conf{$a_src}{$a} <=> $conf{$a_src}{$b}}keys %{$conf{$a_src}})
		{
			my $w2=new wind_db($HOST,$USER,$PSWD);
			$w2->sync(
				"$a_val",
				$THE_GZ_DIR,"",
				\@new_files,
			);
			$w2->logout();
		}
	}
}

#
#
#
if("gz2xml" eq $OP_TP)
{
	foreach my $a_src(sort {$a cmp $b}keys %conf)
	{
print "gz2xml: $a_src\n";
		my $LOCAL_HM="$DATA_HM/$a_src";
		my $THE_GZ_DIR="$LOCAL_HM/gz";
		my $THE_RAW_DIR="$LOCAL_HM/raw";
		mkpath( $THE_GZ_DIR);
		mkpath( $THE_RAW_DIR);

		my $w=new wind_db($HOST,$USER,$PSWD);
		$w->smart_gz2xml($THE_GZ_DIR,$THE_RAW_DIR);
		$w->logout();
	}
}

if("xml2inc" eq $OP_TP)
{
	foreach my $a_src(sort {$a cmp $b}keys %parse_conf)
	{
		my $LOCAL_HM="$DATA_HM/$a_src";
		my $THE_RAW_DIR="$LOCAL_HM/raw";
		my $THE_PS_DIR="$LOCAL_HM/ps";
		my $THE_INC_DIR="$LOCAL_HM/inc";
		mkpath( $THE_RAW_DIR);
		mkpath( $THE_PS_DIR);
		mkpath( $THE_INC_DIR);

print "xml2inc: $a_src\n";
		my $w=new wind_db;
		$w->smart_xml2inc($THE_RAW_DIR,$THE_PS_DIR,$THE_INC_DIR,\%parse_conf,$a_src);
		$w->logout();
	}
}
#############################################################
sub load_conf
{
	my ($conf,$file)=@_;
	open FILE,$file;
	my $the_key="";
	my $the_id=0;
	while(<FILE>)
	{
		chomp;
		if($_=~/^[ \t]*#/){next;}
		if($_=~/^[ \t]*$/){next;}

		if($_=~/^[a-zA-Z]/)
		{
			($the_key)=split /[\t]+/,$_;
			$the_id=0;
		}
		if($_=~/^[\t]+/)
		{
			my ($nouse,$the_val)=split /[\t]+/,$_;
			$the_id++;
			$$conf{$the_key}{$the_val}=$the_id;
		}
	}
	close FILE;

}

sub load_parse_conf
{
	my ($conf,$file)=@_;
	open FILE,$file;
	my $the_key="";
	while(<FILE>)
	{
		chomp;
		if($_=~/^[ \t]*#/){next;}
		if($_=~/^[ \t]*$/){next;}

		if($_=~/^[a-zA-Z]/)
		{
			($the_key)=split /[\t]+/,$_;
		}
		if($_=~/^[\t]+/)
		{
			my ($nouse,$the_var,$the_val)=split /[\t]+/,$_;

			if($the_var eq "header_fields")
			{
				my @elms=split /[ \t]*,[ \t]*/,$the_val;
				my $the_id=0;
				foreach my $an_elm(@elms)
				{
					$the_id++;
					$$conf{$the_key}{$the_var}{$an_elm}=$the_id;
				}
			}else
			{
				my @elms=split /[ \t]*\|[ \t]*/,$the_val;
				my $the_id=1;
				foreach my $an_elm(@elms)
				{
					$$conf{$the_key}{$the_var}{$an_elm}=$the_id;
				}
			}
		}
	}
	close FILE;

}

