#!/usr/bin/perl -w

use Test::More tests => 9;
use FindBin qw($Bin);
use Getopt::Long;

use lib "$Bin/../../../lib";
use lib "$Bin/../../../../../..";

BEGIN {
    use_ok('Test::Engine::Workflow::ExecuteWorkflow'); 
}
require_ok('Test::Engine::Workflow::ExecuteWorkflow');

use Test::Engine::Workflow::ExecuteWorkflow;

#### SET LOGFILE
my $logfile     =   "$Bin/outputs/test.log";
my $log         =   2;
my $printlog    =   5;

my $help;
GetOptions (
    'logfile=s'     =>  \$logfile,
    'log=s'     	=>  \$log,
    'printlog=s'    =>  \$printlog,
    'help'          =>  \$help
) or die "No options specified. Try '--help'\n";
usage() if defined $help;

#### SET CONF
my $configfile  =   "$Bin/../../../../../../../conf/config.yml";
my $conf = Conf::Yaml->new(
	inputfile	=>	$configfile,
	memory		=>	1,
	backup		=>	1,
	separator	=>	"\t",
	spacer		=>	"\\s\+",
    logfile     =>  $logfile,
    log     	=>  2,
    printlog    =>  2
);

my $object = Test::Engine::Workflow::ExecuteWorkflow->new(
    conf            =>  $conf,
    logfile         =>  $logfile,
    log			    =>	$log,
    printlog        =>  $printlog
);
isa_ok($object, "Test::Engine::Workflow::ExecuteWorkflow");

#### TESTS
# $object->testStartStop();
$object->testExecuteWorkflow();
#$object->testUpdateWorkflowStatus();


#### STARCLUSTER
#$object->testLoadStarCluster();
#$object->testStartStarCluster();
#$object->testGetClusterWorkflow();
#$object->testUpdateClusterWorkflow();
#

