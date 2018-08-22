#!/usr/bin/perl -w

=head2
	
APPLICATION 	Common::Engine::Logic::Fork.t

PURPOSE

	Test Engine::Logic::Fork module
	
NOTES

	1. RUN AS ROOT
	
	2. BEFORE RUNNING, SET ENVIRONMENT VARIABLES, E.G.:
	
		export installdir=/aguadev

=cut

use Test::More 	tests => 11;
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../../lib";
BEGIN
{
    my $installdir = $ENV{'installdir'} || "/a";
    unshift(@INC, "$installdir/extlib/lib/perl5");
    unshift(@INC, "$installdir/extlib/lib/perl5/x86_64-linux-gnu-thread-multi/");
    unshift(@INC, "$installdir/lib");
    unshift(@INC, "$installdir/t/unit/lib");
    unshift(@INC, "$installdir/t/common/lib");
}

#### CREATE OUTPUTS DIR
my $outputsdir = "$Bin/outputs";
`mkdir -p $outputsdir` if not -d $outputsdir;

BEGIN {
    use_ok('Conf::Yaml');
    use_ok('Engine::Logic::Fork');
    use_ok('Test::Engine::Logic::Fork');
}
require_ok('Conf::Yaml');
require_ok('Engine::Logic::Fork');
require_ok('Test::Engine::Logic::Fork');

#### SET CONF FILE
my $installdir  =   $ENV{'installdir'} || "/a";
my $urlprefix  =   $ENV{'urlprefix'} || "agua";
my $configfile	=   "$installdir/conf/config.yml";
#print "Installer.t    configfile: $configfile\n";

#### SET $Bin
$Bin =~ s/^.+\/bin/$installdir\/t\/unit\/bin/;

#### GET OPTIONS
my $logfile 	= "/tmp/testuser.login.log";
my $log     =   2;
my $printlog    =   5;
my $help;
GetOptions (
    'log=i'     => \$log,
    'printlog=i'    => \$printlog,
    'logfile=s'     => \$logfile,
    'help'          => \$help
) or die "No options specified. Try '--help'\n";
usage() if defined $help;

my $conf = Conf::Yaml->new(
    inputfile	=>	$configfile,
    memory      =>  1,
    backup	    =>	1,
    separator	=>	"\t",
    spacer	    =>	"\\s\+",
    logfile     =>  $logfile,
	log         =>  2,
	printlog    =>  5    
);
isa_ok($conf, "Conf::Yaml", "conf");

#### SET DUMPFILE
my $dumpfile    =   "$Bin/../../../dump/create.dump";

my $object = new Test::Engine::Logic::Fork(
    conf        =>  $conf,
    logfile     =>  $logfile,
    dumpfile    =>  $dumpfile,
	log			=>	$log,
	printlog    =>  $printlog
);
isa_ok($object, "Test::Engine::Logic::Fork", "object");

#### TESTS
$object->testSelectIndex();
$object->testSelectBranch();
#$object->testBranchedWorkflow();
#$object->testCLIWorkflow();


#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#                                    SUBROUTINES
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

sub usage {
    print `perldoc $0`;
}

