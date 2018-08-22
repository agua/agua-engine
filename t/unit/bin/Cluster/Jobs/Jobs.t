#!/usr/bin/perl -w

use MooseX::Declare;

use strict;

#### EXTERNAL MODULES
use Test::Simple tests => 4;
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../../../../lib";
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


#### SET CONF FILE
my $installdir  =   $ENV{'installdir'} || "/a";
my $configfile  =   "$installdir/conf/config.yml";

#### SET $Bin
$Bin =~ s/^.+t\/bin/$installdir\/t\/bin/;

#### INTERNAL MODULES
use Test::Engine::Cluster::Jobs;
use Conf::Yaml;

#### GET OPTIONS
my $log = 3;
my $printlog = 3;
my $help;
GetOptions (
    'log=i'     => \$log,
    'printlog=i'    => \$printlog,
    'help'          => \$help
) or die "No options specified. Try '--help'\n";
usage() if defined $help;


my $logfile = "$Bin/outputs/testuser.cluster-jobs.log";

my $conf = Conf::Yaml->new(
	inputfile	=>	$configfile,
	backup		=>	1,
	separator	=>	"\t",
	spacer		=>	"\\s\+",
    logfile     =>  $logfile,
    log     	=>  2,
    printlog    =>  2
);

#### SET DUMPFILE
#my $dumpfile = "$Bin/../../../../../bin/sql/dump/agua.dump";
my $dumpfile    =   "$Bin/../../../../dump/create.dump";

my $object = Test::Engine::Cluster::Jobs->new({
    cluster     =>  "SGE",
    dumpfile    =>  $dumpfile,
    conf        =>  $conf,
    username    =>  "testuser",
    logfile     =>  $logfile,
    log			=>	$log,
    printlog    =>  $printlog
});

$object->testCreateTaskDirs();

