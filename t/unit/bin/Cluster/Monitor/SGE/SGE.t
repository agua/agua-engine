#!/usr/bin/perl -w

use strict;

use Test::More  tests => 4;

#### USE LIBS
use FindBin qw($Bin);
use lib "$Bin/../../..//../lib";
BEGIN
{
    my $installdir = $ENV{'installdir'} || "/a";
    unshift(@INC, "$installdir/extlib/lib/perl5");
    unshift(@INC, "$installdir/extlib/lib/perl5/x86_64-linux-gnu-thread-multi/");
    unshift(@INC, "$installdir/lib");
    unshift(@INC, "$installdir/lib/external/lib/perl5");
}

#### CREATE OUTPUTS DIR
my $outputsdir = "$Bin/outputs";
`mkdir -p $outputsdir` if not -d $outputsdir;


#### INTERNAL MODULES
use Test::Engine::Cluster::Monitor::SGE;
use Conf::Yaml;

my $logfile = "/tmp/testuser.monitor.sge.log";
my $log = 2;
my $printlog = 2;

#### SET CONF FILE
my $installdir  =   $ENV{'installdir'} || "/a";
my $configfile    =   "$installdir/conf/config.yml";

#### SET $Bin
$Bin =~ s/^.+t\/bin/$installdir\/t\/bin/;

#### SET DUMPFILE
my $dumpfile    =   "$Bin/../../../../dump/create.dump";
#my $dumpfile    =   "$Bin/../../../../../bin/sql/dump/agua.dump";

my $conf = Conf::Yaml->new(
	inputfile	=>	$configfile,
	backup		=>	1,
	separator	=>	"\t",
	spacer		=>	"\\s\+",
    logfile     =>  $logfile,
	log     =>  2,
	printlog    =>  2
);

my $object = Test::Engine::Cluster::Monitor::SGE->new({
    dumpfile    =>  $dumpfile,
    logfile     =>  $logfile,
	log			=>	$log,
	printlog    =>  $printlog,
    conf        =>  $conf,
    cluster     =>  "SGE",
    username    =>  "admin"    

});

#### CHECK INSTANTIATION OF MONITOR
ok($object->testSetMonitor(), "completed testSetMonitor");

ok($object->testRemainingJobs(), "completed testRemainingJobs");

#### TODO: MORE TESTS WITH TIMERS
#ok($object->testJobLines(), "completed testJobLines");




