#!/usr/bin/perl -w
return TRUE;
package wind_db;
use strict;
use Net::FTP;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;
use File::Copy qw(move);
use POSIX qw(strftime); 
use Data::Dumper;

#
#	初始化，账户登录
#
sub new
{
	my ($class,$HOST,$ACC,$PSWD)= @_;
	my $self ={};
	bless $self, $class;

	my $ftp = Net::FTP->new($HOST, Debug => 0)
     or die "Cannot connect to some.host.name: $@";
	$ftp->login($ACC,$PSWD)
     or die "Cannot authorize ", $ftp->message;
	$ftp->binary;
	$self->{"FTP"}=$ftp;

	return $self;
}

#
#	数据同步，从server到本地
#
sub sync
{
	my ($self,$ftp_path,$gz_path,$prefix,$new_files)=@_;
	my $ftp = $self->{"FTP"};
	if(not ($ftp->cwd("$ftp_path"))){
		print "Cannot change working directory ", $ftp->message;
		return ;
	}
	my @files_ftp = sort {$a cmp $b} $ftp->ls(".")
		or die "ls failed ",$ftp->message;

	opendir(my $dh, $gz_path) || die "can't opendir $gz_path: $!";
	my @files_gz = grep { -f "$gz_path/$_" } readdir($dh);
	closedir $dh;

	my %tmp=();
	foreach my $a_gzfile(@files_gz)
	{
		$tmp{$a_gzfile}=1;
	}

	foreach my $a_ftpfile(@files_ftp)
	{
		my $local_fname="$prefix$a_ftpfile";
		if(not exists $tmp{$local_fname})
		{
print "UPDATE: $local_fname\n";
			my $DOWNLOAD_FNL="$gz_path/$local_fname";
			my $DOWNLOAD_TMP="$DOWNLOAD_FNL.tmp";

			$ftp->get("$ftp_path/$a_ftpfile",$DOWNLOAD_TMP)
				or die "Could not get remotefile:$a_ftpfile /n";
			move $DOWNLOAD_TMP,$DOWNLOAD_FNL;
			$tmp{$local_fname}=1;
push @{$new_files},"$a_ftpfile:$local_fname";
		}
	}
}

#
#
#
sub smart_gz2xml
{
	my ($self,$gz_path,$raw_path)=@_;

	opendir(my $dh, $gz_path) || die "can't opendir $gz_path: $!";
	my @files_gz = grep { $_=~/gz$/ && -f "$gz_path/$_" } readdir($dh);
	my %tmp=();
	foreach my $input(@files_gz)
	{
		my $output = $input;
		$output =~ s/.gz//g;

		my $UNZIP_SRC="$gz_path/$input";
		my $UNZIP_FNL="$raw_path/$output";

		# 对应解压文件没有
		if(not(-f $UNZIP_FNL)){
print "N $input -> $output\n";
			$tmp{$input}=$output;
		}else
		{
			# 对应解压文件没有压缩文件新
			my $SRC_tick=(stat $UNZIP_SRC)[9];
			my $DST_tick=(stat $UNZIP_FNL)[9];

			if($SRC_tick>$DST_tick)
			{
print "R $input -> $output\n";
print "\tS", $SRC_tick,"\n";
print "\tF", $DST_tick,"\n";
				$tmp{$input}=$output;
			}
		}
	}
	closedir $dh;

	foreach my $input(sort {$a cmp $b}keys %tmp)
	{
		my $output=$tmp{$input};
		my $UNZIP_FNL="$raw_path/$output";
		my $UNZIP_TMP="$UNZIP_FNL.tmp";

		gunzip "$gz_path/$input" => "$UNZIP_TMP"
		or die "Error compressing '$input': $GunzipError\n";
print "NEW_XML\t$raw_path/$output\n";
		move $UNZIP_TMP,$UNZIP_FNL;
	}
}

#####################################

sub unzip
{
	my ($self,$gz_path,$raw_path)=@_;

	opendir(my $dh, $raw_path) || die "can't opendir $raw_path: $!";
	my @files_raw = grep { -f "$raw_path/$_" } readdir($dh);
	my %tmp=();
	foreach my $x(@files_raw)
	{
		$tmp{$x}=1;
	}
	closedir $dh;

	opendir(my $dh2, $gz_path) || die "can't opendir $gz_path: $!";
	my @files_gz = grep { $_=~/gz$/ && -f "$gz_path/$_" } readdir($dh2);
	foreach my $input(@files_gz)
	{
		my $output = $input;
		$output =~ s/.gz//g;

		if(exists $tmp{$output}){next;}

print "$input\n\t$output\n";
		gunzip "$gz_path/$input" => "$raw_path/$output"
		or die "Error compressing '$input': $GunzipError\n";
print "NEW_XML\t$raw_path/$output\n";
	}
	closedir $dh2;

		
}

sub logout
{
	my ($self)=@_;
	my $ftp = $self->{"FTP"};
	$ftp->quit;
}

sub read_ftp_dir
{
	my ($self,$ftp_path,$outp_file)=@_;
	
	my $ftp = $self->{"FTP"};

	my @stk=();
	push @stk,$ftp_path;
       
	while(scalar @stk>0)
	{
		my $a_ftp_path=pop @stk;
		$a_ftp_path=~s/[\/]+$//g;

		if(not ($ftp->cwd("$a_ftp_path"))){
			print STDERR "WARNING: Cannot change working directory ", $ftp->message;
			next;
			#return ;
		}

		my @files_ftp = sort {$a cmp $b} $ftp->ls("-lF");
		my %files_map=();
		foreach my $a_tmp(@files_ftp)
		{
			my @x=split /[ \t]+/,$a_tmp;
			my $a_file=$x[-1];

			$files_map{$a_file}=1;
		}

		if(exists $files_map{"FileUpdatedList.xml"})
		{
			print "$a_ftp_path\n";
			next;
		}

		foreach my $a_file(keys %files_map)
		{
			if(not $a_file=~/\/$/){next;}
			$a_file=~s/\/$//g;
print ">>$a_ftp_path/$a_file\n";
			push @stk,"$a_ftp_path/$a_file";
		}
	}


}

sub detoken
{
	my ($line)=@_;
	my $var=$line;
	$var=~s/>.*$//g;
	$var=~s/^[^<]*<//g;
	$line=~s/^[ \t]*<[^\>]+>//g;
	$line=~s/<[^\>]+>[ \t\r]*$//g;
	return ($var,$line);
}

#
#
#
sub smart_xml2inc
{
	my ($self,$xml_path,$ps_path,$inc_path,$CONF,$grp)=@_;

	opendir(my $dh, $xml_path) || die "can't opendir $xml_path: $!";
	my @files_xml = grep { $_=~/xml$/ && -f "$xml_path/$_" } readdir($dh);
	my %tmp=();
	foreach my $input(@files_xml)
	{
#if (not $input =~/20120327/){next;}
		my $output = $input;
		$output =~ s/\.xml$/.ps/g;

		my $PS_SRC="$xml_path/$input";
		my $PS_FNL="$ps_path/$output";

		# 对应目标文件没有
		if(not(-f $PS_FNL)){
print "N $input -> $output\n";
			$tmp{$input}=$output;
		}else
		{
			# 对应目标文件没有源文件新
			my $SRC_tick=(stat $PS_SRC)[9];
			my $DST_tick=(stat $PS_FNL)[9];

			if($SRC_tick>$DST_tick)
			{
print "R $input -> $output\n";
print "\tS", $SRC_tick,"\n";
print "\tF", $DST_tick,"\n";
				$tmp{$input}=$output;
			}
		}
	}
	closedir $dh;

	foreach my $input(sort {$a cmp $b}keys %tmp)
	{
		my $output = $tmp{$input};
		my $PS_SRC="$xml_path/$input";
		my $PS_FNL="$ps_path/$output";
#print "parse_it($CONF,$grp,$PS_SRC,$inc_path)\n";
		$self->parse_it($CONF,$grp,$PS_SRC,$inc_path);
		#touch $PS_FNL;
		open TOUCH, ">$PS_FNL";
		print TOUCH "PS_SRC\t$PS_SRC\n";
		print TOUCH "PS_FNL\t$PS_FNL\n";
		close TOUCH;
	}
}
sub parse_it
{
	my ($self,$CONF,$grp,$IN_FILE,$inc_dir)=@_;
	#my $inc_dir="/data_pool/stock_info/stk_stat/parse_res_inc";

	my $skip_fields=\%{$$CONF{$grp}{"skip_fields"}};
	my $header_fields=\%{$$CONF{$grp}{"header_fields"}};
	my $fileheader_fields=\%{$$CONF{$grp}{"fileheader_fields"}};
	my $stamp_fields=\%{$$CONF{$grp}{"stamp_fields"}};

#print Dumper $header_fields;
#print Dumper $fileheader_fields;

#################################################
	my $LOCAL_PROC="0";
	my $stat=0;
	#my %info=();
	#my %header=();
	#my $timestamp=0;
	#my $fileheader="";
	my %RES=();

#################################################
	my $hid_max=0;
	foreach my $hid(values %{$header_fields})
	{
		if($hid>$hid_max){$hid_max=$hid;}
	}

	my %parse_ctx=();
my $lno=0;
	open XML_FILE,$IN_FILE;
	while(<XML_FILE>)
	{
		chomp;
		$_=~s/[\r\n]*//g;
$lno++;
#print "$lno>$_\n";
		if($_=~/^[ \t]*<Product>/)
		{
			$stat=1;
			#$timestamp=0;
			#%info=();
			#%header=();
			%parse_ctx=(
				"timestamp"=>0,
				"info"=>{},
				"header"=>{},
				"fileheader"=>""
			);
			next;
		}
		if($_=~/^[ \t]*<\/Product>/)
		{
			$stat=0;
	
			my @header_set=();
			foreach my $hid(1..$hid_max)
			{
				#if(exists $header{$hid})
				if(exists $parse_ctx{"header"}{$hid})
				{
					push @header_set,$parse_ctx{"header"}{$hid};
				}else
				{
					push @header_set,"";
				}
			}
	
			my @pv_set=();
			foreach my $a_key(sort {$a cmp $b}keys %{$parse_ctx{"info"}})
			{
				my $a_val=$parse_ctx{"info"}{$a_key};
				push @pv_set,"$a_key\x02$a_val";
			}
	
			my $the_line=join("\t",(
				join("\x03",@header_set)
				,$parse_ctx{"timestamp"}
				,join("\x01",@pv_set)
			));
	
			if(not ($parse_ctx{"fileheader"} eq ""))
			{
				my $fileheader = $parse_ctx{"fileheader"};
				if($LOCAL_PROC eq "1"){print "$the_line\n";}
				else
				{
					#push @{$RES{$fileheader}},$the_line;
					open FILE,">>$inc_dir/$grp.$fileheader.inc";
					print FILE $the_line,"\n";
					close FILE;
#if($the_line=~/002107/)
#{
#print ">>$inc_dir/$grp.$fileheader.inc\t$the_line\n";
#}
				}
			}
			next;
		}
		if(1==$stat)
		{
			$self->parse_inbody($_,\%{$$CONF{$grp}},\%parse_ctx);
		}
	}
	close XML_FILE;

	if($LOCAL_PROC eq "1")
	{
		exit 0;
	}
}

sub parse_inbody
{
	my ($self,$ln,$CONF,$parse_ctx)=@_;
	my $skip_fields=\%{$$CONF{"skip_fields"}};
	my $header_fields=\%{$$CONF{"header_fields"}};
	my $fileheader_fields=\%{$$CONF{"fileheader_fields"}};
	my $stamp_fields=\%{$$CONF{"stamp_fields"}};
	
	$ln=~s/^[ \t]*<//g;
	$ln=~s/>[ \t]*$//g;
	$ln=~s/<\//</g;

	my ($key_b,$val,$key_e)=split /[\<\>]+/,$ln;

	$key_b=~tr/a-z/A-Z/;
	$val=~s/[\x01\x02]+//g;
	$key_e=~tr/a-z/A-Z/;

	if(exists $$skip_fields{$key_b}){return;}

	if(exists $$stamp_fields{$key_b}){
		#2009-01-11T16:12:48+08:00
		my ($YYYY,$MM,$DD,$hh,$mm,$ss)=split /[\-T\:\+]+/,$val;
		my $seconds = Time::Local::timelocal($ss,$mm,$hh,$DD,$MM-1,$YYYY-1900);
		$$parse_ctx{"timestamp"} = $seconds;
		return;
	}

	if(exists $$header_fields{$key_b})
	{
		if(exists $$fileheader_fields{$key_b})
		{
			$$parse_ctx{"fileheader"}=$val;
#if((exists $$parse_ctx{"header"}{"S_INFO_WINDCODE"}) and ("002107.SZ" eq $$parse_ctx{"header"}{"S_INFO_WINDCODE"}))
#{
#print "$key_b>",$$parse_ctx{"fileheader"},"\n";
#}
		}
		my $hid = $$header_fields{$key_b};
		$$parse_ctx{"header"}{$hid}=$val;
	#print ">>$hid\t$val\n";
		return;
	}
	
	if($key_b eq $key_e)
	{
		$$parse_ctx{"info"}{$key_b}=$val;
		return;
	}
}
