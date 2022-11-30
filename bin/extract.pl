#! /usr/bin/env perl
use File::Basename;
my $mypath = dirname($0);
use Time::Local;
use POSIX qw(strftime);	
use lib "/usr/amoeba/lib/perl";
use logging;
$logdir = "$mypath/../log/";
system("mkdir -p $logdir") if (! -d $logdir);
$log = logging->Open("$logdir/extract.log");

my $grib = $ARGV[0];
if (! -f $grib){
    print STDERR "Usage : $0 compass_grib\n";
    exit(1);
}

### GET HEADER INFO
open(my $fh,"<",$grib);
my $hdr;
{local $/ = "\x04\x1a"; $hdr  = <$fh>;}
close($fh);
my %info;
foreach my $ln (split(/[\r\n]/,$hdr)){
    my ($k,$v) = split(/=/,$ln);
    $info{$k} = $v;
}
my $id = '4' . $info{"data_id"};
# created yyyy/mm/dd hh:mn:ss GMT
my ($yy,$mm,$dd,$hh,$nn,$ss) = split(/[^0-9]/,$info{"created"});
my $createdtime = $yy . $mm . $dd . $hh;
print "createdtime $createdtime\n";

($yy,$mm,$dd,$hh,$nn,$ss) = split(/[^0-9]/,$info{"announced"});
my $basetime = $yy . $mm . $dd . $hh;
my $basetm = timegm($ss,$nn,$hh,$dd,$mm-1,$yy);
print "basetime $basetime\n";

my %tagid2perc = (
    #    "412010070" => "60",
    #    "412010071" => "50",
    #    "412010072" => "40",
    "412010073" => "30",
    );

my %target = (
    'APCP'	=> 1,
    );
my $perc = $tagid2perc{$id};
my $spld = "/usr/amoeba/pub/COMPASS-p/grid/$perc/";
system("mkdir -p $spld") if (! -d $spld);

open(P,"/usr/local/bin/wgrib2 -V $grib |") || die $!;
while (<P>) {
    if (/^[0-9]/) {
	my @w = split(/:/,$_);
	my $id = $w[0];
	$vt = $w[2];
	$vt =~ s/vt=//;
	my $el = $w[5];
	$el =~ s/ .*//;
	my $tag = "$el";
	#print "$tag\n";
	if (exists $target{$tag}) {
	    #print "$id, $vt, $el\n";
	    my $outf = "$spld/$tag.$vt";
	    system("/usr/local/bin/wgrib2 -order we:ns -no_header -bin $outf -d $id $grib");
	}
	my ($yy,$mm,$dd,$hh) = unpack('a4 a2 a2 a2', $vt);	
	$tx = timegm(0,0,$hh,$dd,$mm-1,$yy);	
	$ft = ($tx - $basetm) / 3600;
    }
}

open($fout,">","$spld/info.txt");	
print $fout "basetime\t$basetime\n";
print $fout "createdtime\t$createdtime\n";
close($fout);

$log->Info("Generating WxTech file\n");
system("/usr/amoeba/pub/tw_prec/bin/tw_prec_hourly");
$log->Info("Finished generating WxTech file\n");

$log->Info("Syncing to S3\n");
system("/usr/bin/aws s3 sync /usr/amoeba/pub/tw_prec/spl/TW_PREC_1H/ s3://wni-with-wxdata-jp/TW_PREC_1H/ --profile wxtech");
$log->Info("Finished Syncing to S3\n");

$log->Close();

