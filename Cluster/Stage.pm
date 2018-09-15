use MooseX::Declare;

=head2

	PACKAGE		Engine::Cluster::Stage
	
	PURPOSE:
	
		A STAGE IS ONE STEP IN A WORKFLOW. EACH 
		
		STAGE DOES THE FOLLOWING:
		
		1.	RUNS AN ENTRY IN THE stage DATABASE TABLE

		2. LOGS ITS STATUS TO THE stage DATABASE TABLE
		
		3. DYNAMICALLY SETS STDOUT, STDERR, INPUT 

			AND OUTPUT FILES.
		
=cut 

use strict;
use warnings;

#### USE LIB FOR INHERITANCE
use FindBin qw($Bin);
use lib "$Bin/../";

class Engine::Cluster::Stage with (Engine::Common::Stage,
	Util::Logger, 
	Util::Timer, 
	Engine::Cluster::Jobs) {

	# Web::Base, 


#### EXTERNAL MODULES
use Data::Dumper;
use FindBin qw($Bin);

#### INTERNAL MODULES
use Engine::Envar;

# Booleans

# Int/Nums
has 'slots'     	=>  ( isa => 'Int|Undef', is => 'rw' );

# String
has 'clustertype'	=>  ( isa => 'Str|Undef', is => 'rw', default => "SGE" );
has 'qsub'				=>  ( isa => 'Str', is => 'rw' );
has 'qstat'				=>  ( isa => 'Str', is => 'rw' );
has 'resultfile'	=>  ( isa => 'Str', is => 'ro', default => sub { "/tmp/result-$$" });
has 'qsuboptions'			=>  ( isa => 'Str', is => 'rw', required => 1  );
has 'queued'			=>  ( isa => 'Str', is => 'rw' );

# Hash/Array
# has 'customvars'=>	( isa => 'HashRef', is => 'rw', default => sub {
# 	return {
# 		cluster 		=> 	"CLUSTER",
# 		qmasterport 	=> 	"SGE_MASTER_PORT",
# 		execdport 		=> 	"SGE_EXECD_PORT",
# 		sgecell 		=> 	"SGE_CELL",
# 		sgeroot 		=> 	"SGE_ROOT",
# 		queue 			=> 	"QUEUE"
# 	};
# });

# Object
has 'monitor'		=> 	( isa => 'Maybe', is => 'rw', required => 0 );
has 'envars'	=> ( isa => 'HashRef|Undef', is => 'rw', required => 0 );


method BUILD ($args) {
	#$self->logDebug("$$ Stage::BUILD    args:");
	#$self->logDebug("$$ args", $args);
}

method run ($dryrun) {
=head2

	SUBROUTINE		run
	
	PURPOSE

		1. RUN THE STAGE APPLICATION AND UPDATE STATUS TO 'running'
		
		2. UPDATE THE PROGRESS FIELD PERIODICALLY (CHECKPROGRESS OR DEFAULT = 10 SECS)

		3. UPDATE STATUS TO 'complete' WHEN EXECUTED APPLICATION HAS FINISHED RUNNING
		
=cut

	$self->logDebug("dryrun", $dryrun);
	
	#### TO DO: START PROGRESS UPDATER

	#### GET ARGUMENTS ARRAY
  my $stageparameters =	$self->stageparameters();
  $stageparameters =~ s/\'/"/g;
	my $arguments = $self->setArguments($stageparameters);    

	# #### GET PERL5LIB FOR EXTERNAL SCRIPTS TO FIND Agua MODULES
	# my $installdir = $self->conf()->getKey("core:INSTALLDIR");
	# my $perl5lib = "$installdir/lib";
	
	# #### SET EXECUTOR
	# my $executor	.=	"export PERL5LIB=$perl5lib; ";
	# $executor 		.= 	$self->executor() if $self->executor();
	# $self->logDebug("$$ self->executor(): " . $self->executor());

	# #### SET APPLICATION
	# my $application = $self->installdir() . "/" . $self->location();	
	# $self->logDebug("$$ application", $application);

	# #### ADD THE INSTALLDIR IF THE LOCATION IS NOT AN ABSOLUTE PATH
	# $self->logDebug("$$ installdir", $installdir);
	# if ( $application !~ /^\// and $application !~ /^[A-Z]:/i ) {
	# 	$application = "$installdir/bin/$application";
	# 	$self->logDebug("$$ Added installdir to stage_arguments->{location}: " . $application);
	# }

	# #### SET SYSTEM CALL
	# my @systemcall = ($application, @$arguments);
	# my $command = "$executor @systemcall";
	
	#### SET SYSTEM CALL TO POPULATE RUN SCRIPT
	my $systemcall = $self->setSystemCall();
	my $command = join " \\\n", @$systemcall;
	#$self->logDebug("command", $command);

	#### MAIN PARAMS
	my $exitcode = 0;
	my $monitor		=	$self->monitor();	

  #### GET OUTPUT DIR
  my $outputdir = $self->outputdir();
  $self->logDebug("$$ outputdir", $outputdir);

	#### SET JOB NAME AS project-workflow-appnumber
	my $projectname 	= $self->projectname();
	my $workflownumber 	= $self->workflownumber();
	my $workflowname 	= $self->workflowname();
	my $appnumber 		= $self->appnumber();
	my $label =	$projectname;
	$label .= "-" . $workflownumber;
	$label .= "-" . $workflowname;
	$label .= "-" . $appnumber;
  $self->logDebug("$$ label", $label);

	#### SET *** BATCH *** JOB 
	my $job = $self->setJob([$command], $label, $outputdir);
	
	#### GET FILES
	my $commands = $job->{commands};
	my $scriptfile = $job->{scriptfile};
	my $stdoutfile = $job->{stdoutfile};
	my $stderrfile = $job->{stderrfile};
	my $lockfile = $job->{lockfile};
	
	#### PRINT SHELL SCRIPT	
	$self->printScriptfile($scriptfile, $commands, $label, $stdoutfile, $stderrfile, $lockfile);
	$self->logDebug("$$ scriptfile", $scriptfile);

	#### SET QUEUE
	$job->{qsuboptions} = $self->qsuboptions();
	
	#### SET QSUB
	$job->{qsub} = $self->qsub();

	#### SET SGE ENVIRONMENT VARIABLES
	$job->{envars} = $self->envars() if $self->envars();

	#### SUBMIT TO CLUSTER AND GET THE JOB ID 
	my ($jobid, $error)  = $monitor->submitJob($job);
	$self->logDebug("$$ jobid", $jobid);
	$self->logDebug("$$ error", $error);
	return (undef, $error) if not defined $jobid or $jobid =~ /^\s*$/;

	#### SET STAGE PID
	$self->setStagePid($jobid);
	
	#### SET QUEUED
	$self->setQueued();

	#### GET JOB STATUS
	$self->logDebug("$$ Monitoring job...");
	my $jobstatus = $monitor->jobStatus($jobid);
	$self->logDebug("$$ jobstatus", $jobstatus);

	#### SET SLEEP
	my $sleep = $self->conf()->getKey("scheduler:SLEEP");
	$sleep = 5 if not defined $sleep;
	$self->logDebug("$$ sleep", $sleep);
	
	my $set_running = 0;
	while ( $jobstatus ne "completed" and $jobstatus ne "error" ) {
		sleep($sleep);
		$jobstatus = $monitor->jobStatus($jobid);
		$self->setRunning() if $jobstatus eq "running" and not $set_running;
		$set_running = 1 if $jobstatus eq "running";

		$self->setStatus('completed') if $jobstatus eq "completed";
		$self->setStatus('error') if $jobstatus eq "error";
	}
	$self->logDebug("$$ jobstatus", $jobstatus);

	#### PAUSE SEEMS LONG ENOUGH FOR qacct INFO TO BE READY
	my $PAUSE = 2;
	$self->logDebug("$$ Sleeping $PAUSE before self->setRunTimes(jobid)");
	sleep($PAUSE);
	$self->setRunTimes($jobid);

	$self->logDebug("$$ Completed");

	
	#### REGISTER PROCESS IDS SO WE CAN MONITOR THEIR PROGRESS
	$self->registerRunInfo();

	#### SET EMPTY IF UNDEFINED
	$exitcode = "" if not defined $exitcode;
	
	return ($exitcode);
}	#	run

method getField ($field) {
	my $username	=	$self->username();
	my $projectname	=	$self->projectname();
	my $workflowname	=	$self->workflowname();
	my $appnumber	=	$self->appnumber();

	my $query = qq{SELECT $field
FROM stage
WHERE username='$username'
AND projectname='$projectname'
AND workflowname='$workflowname'
AND appnumber='$appnumber'};
	#$self->logDebug("query", $query);
	my $successor = $self->table()->db()->query($query);
	#$self->logDebug("successor", $successor);
	
	return $successor;	
}


method setStageQsuboptions ($qsuboptions) {
	$self->logDebug("qsuboptions", $qsuboptions);
	
	#### GET TABLE KEYS
	my $username 	= $self->username();
	my $projectname 	= $self->projectname();
	my $workflowname 	= $self->workflowname();
	my $appnumber 		= $self->appnumber();
	my $now 		= $self->table()->db()->now();
	my $query = qq{UPDATE stage
SET
qsuboptions = '$qsuboptions'
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};
	$self->logDebug("$query");
	my $success = $self->table()->db()->do($query);
	$self->logDebug("success", $success);
	$self->logError("Could not update stage table with qsuboptions: $qsuboptions") and exit if not $success;
}

method setQueued {
	$self->logDebug("$$ Stage::setQueued(set)");
	my $now = $self->table()->db()->now();
	my $set = qq{
status		=	'queued',
started 	= 	'',
queued 		= 	$now,
completed 	= 	''};
	$self->setFields($set);
}

method setEnvarsub {
	return *_envarSub;
}
	
method _envarSub ($envars, $values, $parent) {
	$self->logDebug("parent: $parent");
	$self->logDebug("envars", $envars);
	$self->logDebug("values", $values);
	#$self->logDebug("SELF->CONF", $self->conf());
	
	#### SET USERNAME AND CLUSTER IF NOT DEFINED
	if ( not defined $values->{sgeroot} ) {
		$values->{sgeroot} = $self->conf()->getKey("scheduler:SGEROOT");
	}
	
	#### SET CLUSTER
	if ( not defined $values->{cluster} and defined $values->{sgecell}) {
		$values->{cluster} = $values->{sgecell};
	}
	
	#### SET QMASTERPORT
	if ( not defined $values->{qmasterport}
		or (
			defined $values->{username}
			and $values->{username}
			and defined $values->{cluster}
			and $values->{cluster}
			and defined $self->table()->db()
			and defined $self->table()->db()->dbh()			
		)
	) {
		$values->{qmasterport} = $parent->getQueueMasterPort($values->{username}, $values->{cluster});
		$values->{execdport} 	= 	$values->{qmasterport} + 1 if defined $values->{qmasterport};
		$self->logDebug("values", $values);
	}
	
	$values->{queue} = $parent->setQueueName($values);
	$self->logDebug("values", $values);
	
	return $self->values($values);	
}


method getQueueMasterPort ($username, $cluster) {
	my $query = qq{SELECT qmasterport
FROM clustervars
WHERE username = '$username'
AND cluster = '$cluster'};
	$self->logDebug("query", $query);
		
	return $self->table()->db()->query($query);
}

method setQueueName ($values) {
	$self->logDebug("values", $values);
	return if not defined $values->{username};
	return if not defined $values->{projectname};
	return if not defined $values->{workflowname};
	
	return $values->{username} . "." . $values->{projectname} . "." . $values->{workflowname};
}


} #### Engine::Stage

