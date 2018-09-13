use MooseX::Declare;

=head2

	PACKAGE		Engine::Local::Stage
	
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

class Engine::Local::Stage with (Engine::Common::Stage,
	Util::Logger, 
	Util::Timer) {

	# Web::Base, 


#### EXTERNAL MODULES
use IO::Pipe;
use Data::Dumper;
use FindBin qw($Bin);

#### INTERNAL MODULES
use Engine::Envar;

# Booleans

# Int/Nums

# String

# Hash/Array

# Object


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

	#### REGISTER PROCESS IDS SO WE CAN MONITOR THEIR PROGRESS
	$self->registerRunInfo();

	#### SET SYSTEM CALL TO POPULATE RUN SCRIPT
	my $systemcall = $self->setSystemCall();

	#### REDIRECTION IS 1 IF SYSTEM CALL CONTAINS A ">"
	my $redirection	=	$self->containsRedirection($systemcall);
	$self->logDebug("redirection", $redirection);
	
	#### SET STDOUT AND STDERR FILES
	my $stdoutfile = $self->stdoutfile();
	my $stderrfile = $self->stderrfile();
	push @$systemcall, " \\\n1> $stdoutfile" if defined $stdoutfile and $redirection ne "stdout";
	push @$systemcall, " \\\n2> $stderrfile" if defined $stderrfile and $redirection ne "stderr";
	#$self->logDebug("$$ systemcall: @systemcall");

	#### COMMAND
	my $command = join " \\\n", @$systemcall;
	#$self->logDebug("command", $command);
	
	#### CLEAN UP BEFOREHAND
	`rm $stdoutfile` if -f $stdoutfile;
	`rm $stderrfile` if -f $stderrfile;
	
	#### CREATE stdout DIR
	$self->logDebug("stdoutfile", $stdoutfile);
	my ($outputdir, $label)	=	$stdoutfile	=~	/^(.+?)\/[^\/]+\/([^\/]+)\.stdout$/;
	$self->logDebug("outputdir", $outputdir);
	$self->logDebug("label", $label);

	my $scriptfile	=	"$outputdir/script/$label.sh";
	my $exitfile	=	"$outputdir/stdout/$label.exit";
	my $lockfile	=	"$outputdir/stdout/$label.lock";
	$self->logDebug("scriptfile", $scriptfile);
	#$self->logDebug("exitfile", $exitfile);
	#$self->logDebug("lockfile", $lockfile);

	$self->printScriptFile($scriptfile, $command, $exitfile, $lockfile);
	
	#### UPDATE STATUS TO 'running'
	$self->setRunningStatus();

	#### NO BUFFERING
	$| = 1;

	#### RUN
	$self->logDebug("PID $$ BEFORE SUBMIT command");
	`$scriptfile`;
	$self->logDebug("PID $$ AFTER SUBMIT command");
	
	#### DISABLE STDOUT BUFFERING ON PARENT
	$| = 1;
	
	#### PAUSE FOR RESULT FILE TO BE WRITTEN 
	sleep($self->runsleep());
	$self->logDebug("PID $$ Finished wait for command to complete");
	open(RESULT, $exitfile);
	my $exitcode = <RESULT>;
	close(RESULT);
	$exitcode =~ s/\s+$//;
	#$self->logDebug("PID $$ exitfile", $exitfile);
	$self->logDebug("PID $$ exitcode", $exitcode);
	
	#### SET STATUS TO 'error' IF exitcode IS NOT ZERO
	if ( defined $exitcode and $exitcode == 0 ) {
		$self->setStatus('completed') ;
	}
	else {
		$self->setStatus('error');
	}
	$exitcode	=~ 	s/\s+$// if defined $exitcode;
	$self->logDebug("FIRST exitcode", $exitcode);
	
	return $exitcode;
}

method containsRedirection ($arguments) {
	return if not defined $arguments or not @$arguments;
	
	foreach my $argument ( @$arguments ) {
		return "stdout" if $argument eq ">" or $argument eq "1>";
		return "stderr" if $argument eq "2>";
	}
	
	return 0;
}

method getPreScript ($file) {
	$self->logDebug("file", $file);
  open(FILE, $file) or die "Can't open file: $file: $!";

	my $exports	=	"";
  while ( <FILE> ) {
		next if $_	=~ /^#/ or $_ =~ /^\s*$/;
		chomp;
		$exports .= "$_; ";
  }

	return $exports;
}

method setStageJob {

=head2

	SUBROUTINE		setStageJob
	
	PURPOSE
	
		RETURN THE JOB HASH FOR THIS STAGE:
		
			command		:	Command line system call,
			label		:	Unique name for job (e.g., to be used by SGE)
			outputfile	:	Location of outputfile

=cut

	#$self->logCaller("");

	#### CLUSTER MONITOR
	my $monitor		=	$self->monitor();	
	#### GET MAIN PARAMS
	my $username 	= $self->username();
	my $projectname 	= $self->projectname();
	my $workflownumber 	= $self->workflownumber();
	my $workflowname 	= $self->workflowname();
	my $appnumber 		= $self->appnumber();
	my $queue 		= $self->queue();
	my $cluster		= $self->cluster();
	my $qstat		= $self->qstat();
	my $qsub		= $self->qsub();
	my $workflowpid = $self->workflowpid();
    #$self->logDebug("$$ cluster", $cluster);

	#### SET DEFAULTS
	$queue = '' if not defined $queue;

	#### GET AGUA DIRECTORY FOR CREATING STDOUTFILE LATER
	my $aguadir 	= $self->conf()->getKey("core:AGUADIR");

	#### GET FILE ROOT
	my $fileroot = $self->util()->getFileroot($username);

	#### GET ARGUMENTS ARRAY
    my $stageparameters =	$self->stageparameters();
    #$self->logDebug("$$ Arguments", $stageparameters);
    $stageparameters =~ s/\'/"/g;
	my $arguments = $self->setArguments($stageparameters);    

	#### GET PERL5LIB FOR EXTERNAL SCRIPTS TO FIND Agua MODULES
	my $installdir = $self->conf()->getKey("core:INSTALLDIR");
	my $perl5lib = "$installdir/lib";
	
	#### SET EXECUTOR
	my $executor	.=	"export PERL5LIB=$perl5lib; ";
	$executor 		.= 	$self->executor() if defined $self->executor();
	#$self->logDebug("$$ self->executor(): " . $self->executor());

	#### SET APPLICATION
	my $application = $self->installdir() . "/" . $self->location();	
	#$self->logDebug("$$ application", $application);

	#### ADD THE INSTALLDIR IF THE LOCATION IS NOT AN ABSOLUTE PATH
	#$self->logDebug("$$ installdir", $installdir);
	if ( $application !~ /^\// and $application !~ /^[A-Z]:/i ) {
		$application = "$installdir/bin/$application";
		#$self->logDebug("$$ Added installdir to stage_arguments->{location}: " . $application);
	}

	#### SET SYSTEM CALL
	my @systemcall = ($application, @$arguments);
	my $command = "$executor @systemcall";
	
    #### GET OUTPUT DIR
    my $outputdir = $self->outputdir();
    #$self->logDebug("$$ outputdir", $outputdir);

	#### SET JOB NAME AS projectname-workflowname-appnumber
	my $label =	$projectname;
	$label .= "-" . $workflownumber;
	$label .= "-" . $workflowname;
	$label .= "-" . $appnumber;
    #$self->logDebug("$$ label", $label);
	
	my $samplehash	=	$self->samplehash();
	$self->logNote("samplehash", $samplehash);
	if ( defined $samplehash ) {
		my $id		=	$samplehash->{sample};
		$label		=	"$id.$label";
	}

	#### SET JOB 
	return $self->setJob([$command], $label, $outputdir);
}

method updateStatus ($set, $username, $projectname, $workflowname) {
	
	my $query = qq{UPDATE stage
SET $set
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
};
	$self->logDebug("$$ $query");
	my $success = $self->table()->db()->do($query);
	if ( not $success )
	{
		$self->logError("Can't update stage table for username $username, project $projectname, workflow $workflowname with set clause: $set");
		exit;
	}
}

method printScriptFile ($scriptfile, $command, $exitfile, $lockfile) {
	$self->logNote("scriptfile", $scriptfile);

	#### CREATE DIR COMMANDS
	$self->mkdirCommand($scriptfile);
	$self->mkdirCommand($exitfile);
	$self->mkdirCommand($lockfile);

	my $contents	=	qq{#!/bin/bash

echo "-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
echo USERNAME: 	     	\$USERNAME
echo PROJECT:  	       	\$PROJECT
echo WORKFLOW: 	       	\$WORKFLOW
echo QUEUE:    	       	\$QUEUE
echo "-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"

hostname -f
date

# OPEN LOCKFILE
date > $lockfile

$command

#### REMOVE LOCKFILE
echo \$? > $exitfile

# REMOVE LOCKFILE
unlink $lockfile;

exit 0;

};
	$self->logDebug("contents", $contents);

	open(OUT, ">$scriptfile") or die "Can't open script file: $scriptfile\n";
	print OUT $contents;
	close(OUT);
	chmod(0777, $scriptfile);
	$self->logNote("scriptfile printed", $scriptfile);
}

method mkdirCommand ($file) {
	my ($dir)	=	$file	=~	/^(.+?)\/[^\/]+$/;
	my $command	=	"mkdir -p $dir";
	#$self->logDebug("command", $command);
	
	`$command`;
}

method registerRunInfo {
=head2

	SUBROUTINE		registerRunInfo
	
	PURPOSE
	
		SET THE PROCESS IDS FOR:
		
			- THE STAGE ITSELF
			
			- THE PARENT OF THE STAGE'S APPLICATION (SAME AS STAGE)
		
			- THE CHILD OF THE STAGE'S APPLICATION
		
=cut
	$self->logDebug("$$ Engine::Stage::registerRunInfo()");

	my $workflowpid = $self->workflowpid();
	my $stagepid 	= $self->stagepid() || '';
	my $stagejobid = $self->stagejobid() || '';
	my $username 	= $self->username();
	my $projectname 	= $self->projectname();
	my $workflowname 	= $self->workflowname();
	my $workflownumber = $self->workflownumber();
	my $appnumber		 = $self->appnumber();
	my $stdoutfile		 = $self->stdoutfile();
	my $stderrfile		 = $self->stderrfile();
	#### UPDATE status TO waiting IN TABLE stage
    my $query = qq{UPDATE stage
    SET
	stdoutfile='$stdoutfile',
	stderrfile='$stderrfile'
	WHERE username = '$username'
    AND projectname = '$projectname'
    AND workflowname = '$workflowname'
    AND workflownumber = '$workflownumber'
    AND appnumber = '$appnumber'};
    my $success = $self->table()->db()->do($query);
	if ( not $success )
	{
		$self->logDebug("$$ Could not insert entry for stage $self->stagenumber() into 'stage' table");
        return 0;
    }

	return 1;
}

method register ( $status ) {
#### SET STATUS TO waiting FOR A STAGE IN THE stage TABLE
	$self->logDebug("PID $$");
    
	#### SET SELF _status TO waiting
	$self->status('waiting');

	my $username = $self->username();
	my $projectname = $self->projectname();
	my $workflowname = $self->workflowname();
	my $workflownumber = $self->workflownumber();
	my $appnumber = $self->appnumber();

	#### UPDATE status TO waiting IN TABLE stage
    my $query = qq{UPDATE stage
    SET status='waiting'
	WHERE username = '$username'
    AND projectname = '$projectname'
    AND workflowname = '$workflowname'
    AND workflownumber = '$workflownumber'
    AND appnumber = '$appnumber'};
    $self->logDebug("$$ $query");
    my $success = $self->table()->db()->do($query);
	$self->logDebug("$$ insert success", $success);
	if ( not $success )
	{
		warn "Stage::register    Could not insert entry for stage $self->stagenumber() into 'stage' table\n";
        return 0;
    }

	$self->logDebug("$$ Successful insert!");
	return 1;
}

method isComplete {
=head2

	SUBROUTINE		isComplete
	
	PURPOSE

		CHECK IF THIS STAGE HAS STATUS 'complete' IN THE stage
		
	INPUT
	
		WORKFLOW NAME (workflowname) AND STAGE NAME (appname)
	
	OUTPUT
	
		RETURNS 1 IF COMPLETE, 0 IF NOT COMPLETE
	
=cut

    
	my $projectname = $self->projectname();
	my $workflowname = $self->workflowname();
	my $appnumber = $self->appnumber();

	my $query = qq{SELECT status
	FROM stage
	WHERE projectname='$projectname'
	AND workflowname = '$workflowname'
	AND appnumber = '$appnumber'
	AND status='completed'};
	$self->logDebug("$$ $query");
	my $complete = $self->table()->db()->query($query);
	$self->logDebug("$$ complete", $complete);
	
	return 0 if not defined $complete or not $complete;
	return 1;
}


method initialiseRunTimes ($mysqltime) {
	$self->logDebug("mysqltime", $mysqltime);
	my $set = qq{
queued = '$mysqltime',
started = '$mysqltime',
completed = ''};
	#$self->logDebug("$$ set", $set);

	$self->setFields($set);
}

method setRunTimes ($jobid) {
	$self->logDebug("$$ Stage::setRunTimes(jobid)");
	$self->logDebug("$$ jobid", $jobid);
	my $username = $self->username();
	my $cluster = $self->cluster();
	my $qacct = $self->monitor()->qacct($username, $cluster, $jobid);
	$self->logDebug("$$ qacct", $qacct);

	return if not defined $qacct or not $qacct;
	return if $qacct =~ /^error: job id \d+ not found/;

	#### QACCT OUTPUT FORMAT:
	#	qsub_time    Sat Sep 24 01:05:17 2011
	#	start_time   Sat Sep 24 01:05:24 2011
	#	end_time     Sat Sep 24 01:05:24 2011

	my ($queued) = $qacct =~ /qsub_time\s+([^\n]+)/ms;
	my ($started) = $qacct =~ /start_time\s+([^\n]+)/ms;
	my ($completed) = $qacct =~ /end_time\s+([^\n]+)/ms;
	$queued = $self->datetimeToMysql($queued);
	$started = $self->datetimeToMysql($started);
	$completed = $self->datetimeToMysql($completed);
	
	my $set = qq{
queued = '$queued',
started = '$started',
completed = '$completed'};
	$self->logDebug("$$ set", $set);

	$self->setFields($set);
}
method setRunningStatus {
	my $now = $self->table()->db()->now();
	my $set = qq{status='running',
started=$now,
queued=$now,
completed='0000-00-00 00:00:00'};
	$self->setFields($set);
}

method setStatus ($status) {	
#### SET THE status FIELD IN THE stage TABLE FOR THIS STAGE
    $self->logDebug("$$ status", $status);

	#### GET TABLE KEYS
	my $username = $self->username();
	my $projectname = $self->projectname();
	my $workflowname = $self->workflowname();
	my $appnumber = $self->appnumber();
	my $completed = "completed=" . $self->table()->db()->now();
	# my $completed = "completed = DATE_SUB(NOW(), INTERVAL 3 SECOND)";
	$completed = "completed=''" if $status eq "running";
	
	my $query = qq{UPDATE stage
SET
status = '$status',
$completed
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};
	$self->logNote("$query");
	my $success = $self->table()->db()->do($query);
	if ( not $success )
	{
		$self->logError("Can't update stage (project: $projectname, workflow: $workflowname, number: $appnumber) with status: $status");
		exit;
	}
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

method setRunning {
	$self->logDebug("$$ Stage::setRunning(set)");
	my $now = $self->table()->db()->now();
	my $set = qq{
status		=	'running',
started 	= 	$now,
completed 	= 	''};
	$self->setFields($set);
}

method setFields ($set) {
    #$self->logDebug("set", $set);

	#### GET TABLE KEYS
	my $username 	= 	$self->username();
	my $projectname 	= 	$self->projectname();
	my $workflowname 	= 	$self->workflowname();
	my $appnumber 		= 	$self->appnumber();
	my $now 		= 	$self->table()->db()->now();

	my $query = qq{UPDATE stage
SET $set
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};	
	#$self->logDebug("$query");
	my $success = $self->table()->db()->do($query);
	$self->logError("Could not set fields for stage (project: $projectname, workflow: $workflowname, number: $appnumber) set : '$set'") and exit if not $success;
}

method setStagePid ($stagepid) {
	$self->logDebug("stagepid", $stagepid);
	
	#### GET TABLE KEYS
	my $username 	= $self->username();
	my $projectname 	= $self->projectname();
	my $workflowname 	= $self->workflowname();
	my $appnumber 		= $self->appnumber();
	my $now 		= $self->table()->db()->now();
	my $query = qq{UPDATE stage
SET
stagepid = '$stagepid'
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND appnumber = '$appnumber'};
	$self->logDebug("$query");
	my $success = $self->table()->db()->do($query);
	$self->logDebug("success", $success);
	$self->logError("Could not update stage table with stagepid: $stagepid") and exit if not $success;
}

method toString {
	print $self->_toString();
}

method _toString {
	my @keys = qw[ username projectname workflownumber workflowname appname appnumber start executor location fileroot queue queue_options outputdir scriptfile stdoutfile stderrfile workflowpid stagepid stagejobid submit setuid installdir cluster qsub qstat resultfile];
	my $string = '';
	foreach my $key ( @keys )
	{
		my $filler = " " x (20 - length($key));
		$string .= "$key$filler:\t";
		$string .= $self->$key() || '';
		$string .= "\n";
	}
	$string .= "\n\n";
}


#### ENVAR
	

} #### Engine::Stage

