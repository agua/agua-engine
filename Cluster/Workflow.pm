use MooseX::Declare;

use strict;
use warnings;
use Carp;

class Engine::Cluster::Workflow with (Engine::Common::Workflow,
	Util::Logger, 
	Util::Timer) {

#### EXTERNAL MODULES
use Data::Dumper;
use FindBin::Real;
use lib FindBin::Real::Bin() . "/lib";
use TryCatch;

##### INTERNAL MODULES	
use DBase::Factory;
use Conf::Yaml;
use Engine::Cluster::Stage;
use Engine::Cluster::Monitor::SGE;
use Engine::Cloud::Instance;
use Engine::Envar;
use Table::Main;
use Exchange::Main;

# Bool

# Int
has 'qmasterport'	=> 	( isa => 'Int', is  => 'rw' );
has 'execdport'		=> 	( isa => 'Int', is  => 'rw' );
has 'maxjobs'			=> 	( isa => 'Int', is => 'rw'	);

# Str
has 'samplename'     	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'scheduler'	 	=> 	( isa => 'Str|Undef', is => 'rw', default	=>	"local");
has 'qstat'				=> 	( isa => 'Str|Undef', is => 'rw', default => '' );
has 'queue'				=>  ( isa => 'Str|Undef', is => 'rw', default => 'default' );
has 'cluster'			=>  ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'keypairfile'	=> 	( isa => 'Str|Undef', is  => 'rw', required	=>	0	);
has 'keyfile'			=> 	( isa => 'Str|Undef', is => 'rw'	);
has 'instancetype'=> 	( isa => 'Str|Undef', is  => 'rw', required	=>	0	);
has 'sgeroot'			=> 	( isa => 'Str', is  => 'rw', default => "/opt/sge6"	);
has 'sgecell'			=> 	( isa => 'Str', is  => 'rw', required	=>	0	);
has 'upgradesleep'=> 	( isa => 'Int', is  => 'rw', default	=>	10	);

# Object
has 'data'				=> 	( isa => 'HashRef|Undef', is => 'rw', default => undef );
has 'samplehash'	=> 	( isa => 'HashRef|Undef', is => 'rw', required	=>	0	);
has 'jsonparser'	=> 	( isa => 'JSON', is => 'rw', lazy => 1, builder => "setJsonParser" );
has 'json'				=> 	( isa => 'HashRef', is => 'rw', required => 0 );

has 'head'				=> 	( isa => 'Engine::Cloud::Instance', is => 'rw', lazy => 1, builder => "setHead" );
has 'master'			=> 	( isa => 'Engine::Cloud::Instance', is => 'rw', lazy => 1, builder => "setMaster" );

has 'monitor'			=> 	( isa => 'Engine::Cluster::Monitor::SGE|Undef', is => 'rw', lazy => 1, builder => "setMonitor" );

has 'worker'			=> 	( isa => 'Maybe', is => 'rw', required => 0 );

has 'envarsub'	=> ( isa => 'Maybe', is => 'rw' );
has 'customvars'=>	( isa => 'HashRef', is => 'rw' );


method initialise ($data) {
	#### SET LOG
	my $username 	=	$data->{username};
	my $logfile 	= 	$data->{logfile};
	my $mode		=	$data->{mode};
	$self->logDebug("logfile", $logfile);
	$self->logDebug("mode", $mode);
	if ( not defined $logfile or not $logfile ) {
		my $identifier 	= 	"workflow";
		$self->setUserLogfile($username, $identifier, $mode);
		$self->appendLog($logfile);
	}

	#### ADD data VALUES TO SLOTS
	$self->data($data);
	if ( $data ) {
		foreach my $key ( keys %{$data} ) {
			#$data->{$key} = $self->unTaint($data->{$key});
			$self->$key($data->{$key}) if $self->can($key);
		}
	}
	#$self->logDebug("data", $data);	
	
	#### SET DATABASE HANDLE
	$self->logDebug("Doing self->setDbh");
	$self->setDbObject( $data ) if not defined $self->table()->db();
    
	#### SET WORKFLOW PROCESS ID
	$self->workflowpid($$);	

	#### SET CLUSTER IF DEFINED
	$self->logError("Engine::Workflow::BUILD    conf->getKey(agua, CLUSTERTYPE) not defined") if not defined $self->conf()->getKey("core:CLUSTERTYPE");    
	$self->logError("Engine::Workflow::BUILD    conf->getKey(cluster, QSUB) not defined") if not defined $self->conf()->getKey("scheduler:QSUB");
	$self->logError("Engine::Workflow::BUILD    conf->getKey(cluster, QSTAT) not defined") if not defined $self->conf()->getKey("scheduler:QSTAT");
}

method executeProject {
	my $database 		=	$self->database();
	my $username 		=	$self->username();
	my $projectname =	$self->projectname();
	$self->logDebug("username", $username);
	$self->logDebug("projectname", $projectname);
	
	my $fields	=	["username", "projectname"];
	my $data	=	{
		username	=>	$username,
		projectname		=>	$projectname
	};
	my $notdefined = $self->table()->db()->notDefined($data, $fields);
	$self->logError("undefined values: @$notdefined") and return 0 if @$notdefined;
	
	#### RETURN IF RUNNING
	$self->logError("Project is already running: $projectname") and return if $self->projectIsRunning($username, $projectname);
	
	#### GET WORKFLOWS
	my $workflows	=	$self->table()->getWorkflowsByProject({
		username			=>	$username,
		projectname		=>	$projectname
	});
	$self->logDebug("workflows", $workflows);
	
	#### RUN WORKFLOWS
	my $success	=	1;
	foreach my $object ( @$workflows ) {
		$self->logDebug("object", $object);
		$self->username($username);
		$self->projectname($projectname);
		my $workflowname	=	$object->{name};
		$self->logDebug("workflowname", $workflowname);
		$self->workflowname($workflowname);
	
		#### RUN 
		try {
			$success	=	$self->executeWorkflow();		
		}
		catch {
			print "Workflow::runProjectWorkflows   ERROR: failed to run workflowname '$workflowname': $@\n";
			$self->setProjectStatus("error");
			#$self->notifyError($object, "failed to run workflowname '$workflowname': $@");
			return 0;
		}
	}
	$self->logGroupEnd("Agua::Project::executeProject");
	
	return $success;
}

#### EXECUTE WORKFLOW IN SERIES
method executeWorkflow ($data) {
	$self->logNote("data", $data);

	my $username 			=	$data->{username};
	my $cluster 			=	$data->{cluster};
	my $projectname 	=	$data->{projectname};
	my $workflowname 	=	$data->{workflowname};
	my $workflownumber=	$data->{workflownumber};
	my $samplehash 		=	$data->{samplehash};
	my $submit 				= $data->{submit};
	my $start					=	$data->{start};
	my $stop					=	$data->{stop};
	my $dryrun				=	$data->{dryrun};
	my $queue					=	$data->{queue};
	my $scheduler			=	$self->conf()->getKey("core:SCHEDULER");
	my $force 				=	$self->force() || $data->{force};
	$self->logDebug("force", $force);
	$self->force($force);
	
	$self->logDebug("submit", $submit);
	$self->logDebug("username", $username);
	$self->logDebug("projectname", $projectname);
	$self->logDebug("workflowname", $workflowname);
	$self->logDebug("workflownumber", $workflownumber);
	$self->logDebug("cluster", $cluster);
	$self->logDebug("start", $start);
	$self->logDebug("stop", $stop);
	$self->logDebug("dryrun", $dryrun);
	$self->logDebug("scheduler", $scheduler);

	#### QUIT IF INSUFFICIENT INPUTS
	if ( not $username or not $projectname or not $workflowname or not $workflownumber or not defined $start ) {
		my $error = '';
		$error .= "username, " if not defined $username;
		$error .= "projectname, " if not defined $projectname;
		$error .= "workflowname, " if not defined $workflowname;
		$error .= "workflownumber, " if not defined $workflownumber;
		$error .= "start, " if not defined $start;
		$error =~ s/,\s+$//;
		# $self->notifyError($data, $error) if $exchange eq "true";
		return;
	}

	#### SET SCHEDULER
	$self->scheduler($scheduler);

	$data = {
		username				=>	$username,
		projectname			=>	$projectname,
		workflowname		=>	$workflowname,
		workflownumber	=> 	$workflownumber,
		start						=>	$start,
		samplehash			=>	$samplehash
	};

	#### SET WORKFLOW 'RUNNING'
	$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, "running");

	#### SET STAGES
	$self->logDebug("DOING self->setStages");
	my $stages = $self->setStages($username, $cluster, $data, $projectname, $workflowname, $workflownumber, $samplehash, $scheduler, $queue);
	$self->logDebug("no. stages", scalar(@$stages));
	if ( scalar(@$stages) == 0 ) {
		print "Skipping workflow: $workflowname\n";
		return;
	}

	#### NOTIFY RUNNING
	print "Running workflow $projectname.$workflowname\n";
	my $status;

	$self->logDebug("DOING self->runSge");
	my $success	=	$self->runWorkflow($stages, $username, $projectname, $workflowname, $workflownumber, $cluster);
	$self->logDebug("success", $success);

	#### SET WORKFLOW STATUS
	$status		=	"completed";
	$status		=	"error" if not $success;
	$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, $status);

	#### ADD QUEUE SAMPLE
	my $uuid	=	$samplehash->{samplename};
	$self->logDebug("uuid", $uuid);
	if ( defined $uuid ) {
		$success	=	$self->addQueueSample($uuid, $status, $data);
		$self->logDebug("addQueueSample success", $success);	
	}

	#### NOTIFY COMPLETED
	print "Completed workflow $projectname.$workflowname\n";

	$self->logGroupEnd("$$ Engine::Workflow::executeWorkflow    COMPLETED");
}


method runWorkflow ($stages, $username, $projectname, $workflowname, $workflownumber, $cluster) {	
#### RUN STAGES ON SUN GRID ENGINE

	my $sgeroot	=	$self->conf()->getKey("scheduler:SGEROOT");
	my $celldir	=	"$sgeroot/$workflowname";
	$self->logDebug("celldir", $celldir);
	# $self->_newCluster($username, $workflowname) if not -d $celldir;

	# #### CREATE UNIQUE QUEUE FOR WORKFLOW
	# my $envar = $self->envar($username, $cluster);
	# $self->logDebug("envar", $envar);
	# $self->createQueue($username, $cluster, $projectname, $workflowname, $envar);

	# #### SET CLUSTER WORKFLOW STATUS TO 'running'
	# $self->updateClusterWorkflow($username, $cluster, $projectname, $workflowname, 'running');
	
	#### SET WORKFLOW STATUS TO 'running'
	# $self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'running');
	
	# ### RELOAD DBH
	# $self->setDbh();
	
	#### RUN STAGES
	$self->logDebug("BEFORE runStages()\n");
	my $dryrun = $self->dryrun();
	my $success	=	$self->runStages($stages, $dryrun);
	$self->logDebug("AFTER runStages    success: $success\n");
	
	# #### RESET DBH JUST IN CASE
	# $self->setDbh();
	
	if ( $success == 0 ) {
		# #### SET CLUSTER WORKFLOW STATUS TO 'completed'
		# $self->updateClusterWorkflow($username, $cluster, $projectname, $workflowname, 'error');
		
		#### SET WORKFLOW STATUS TO 'completed'
		$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'error');
	}
	else {
		# #### SET CLUSTER WORKFLOW STATUS TO 'completed'
		# $self->updateClusterWorkflow($username, $cluster, $projectname, $workflowname, 'completed');
	
		#### SET WORKFLOW STATUS TO 'completed'
		$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'completed');
	}
}

#### UPDATE
method runInParallel ($workflowhash, $sampledata) {
=head2

	SUBROUTINE		executeCluster
	
	PURPOSE
	
		EXECUTE A LIST OF JOBS CONCURRENTLY UP TO A MAX NUMBER
		
		OF CONCURRENT JOBS

=cut

	$self->logCaller("");

	my $username 	=	$self->username();
	my $cluster 	=	$self->cluster();
	my $projectname 	=	$self->projectname();
	my $workflowname 	=	$self->workflowname();
	my $workflownumber=	$self->workflownumber();
	my $start 		=	$self->start();
	my $submit 		= 	$self->submit();
	$self->logDebug("submit", $submit);
	$self->logDebug("username", $username);
	$self->logDebug("projectname", $projectname);
	$self->logDebug("workflowname", $workflowname);
	$self->logDebug("workflownumber", $workflownumber);
	$self->logDebug("cluster", $cluster);
	
	print "Running workflow $projectname.$workflowname\n";

	#### GET CLUSTER
	$cluster		=	$self->getClusterByWorkflow($username, $projectname, $workflowname) if $cluster eq "";
	$self->logDebug("cluster", $cluster);	
	$self->logDebug("submit", $submit);	
	
	#### RUN LOCALLY OR ON CLUSTER
	my $scheduler	=	$self->scheduler() || $self->conf()->getKey("core:SCHEDULER");
	$self->logDebug("scheduler", $scheduler);

	#### GET ENVIRONMENT VARIABLES
	my $envar = $self->envar();
	#$self->logDebug("envar", $envar);

	#### GET STAGES
	my $samplehash	=	undef;
	my $stages	=	$self->setStages($username, $cluster, $workflowhash, $projectname, $workflowname, $workflownumber, $samplehash, $scheduler);
	$self->logDebug("no. stages", scalar(@$stages));
	#$self->logDebug("stages", $stages);

	#### GET FILEROOT
	my $fileroot = $self->util()->getFileroot($username);	
	$self->logDebug("fileroot", $fileroot);

	#### GET OUTPUT DIR
	my $outputdir =  "$fileroot/$projectname/$workflowname/";
	
	#### GET MONITOR
	my $monitor	=	$self->updateMonitor() if $scheduler eq "sge";

	#### SET FILE DIRS
	my ($scriptdir, $stdoutdir, $stderrdir) = $self->setFileDirs($fileroot, $projectname, $workflowname);
	$self->logDebug("scriptdir", $scriptdir);
	
	#### WORKFLOW PROCESS ID
	my $workflowpid = $self->workflowpid();

	$self->logDebug("DOING ALL STAGES stage->setStageJob()");
	foreach my $stage ( @$stages )  {
		#$self->logDebug("stage", $stage);
		my $installdir		=	$stage->installdir();
		$self->logDebug("installdir", $installdir);

		my $jobs	=	[];
		foreach my $samplehash ( @$sampledata ) {
			$stage->{samplehash}	=	$samplehash;
			
			push @$jobs, $stage->setStageJob();
		}
		$self->logDebug("no. jobs", scalar(@$jobs));

		#### SET LABEL
		my $stagename	=	$stage->name();
		$self->logDebug("stagename", $stagename);
		my $label	=	"$projectname.$workflowname.$stagename";

		$stage->runJobs($jobs, $label);
	}

	print "Completed workflow $projectname.$workflowname\n";

	$self->logDebug("COMPLETED");
}

method runStages ($stages, $dryrun) {
	$self->logDebug("no. stages", scalar(@$stages));

	# #### SET EXCHANGE	
	# my $exchange = $self->conf()->getKey("core:EXCHANGE");
	# $self->logDebug("exchange", $exchange);
	
	#### SELF IS SIPHON WORKER
	my $worker	=	0;
	$worker		=	1 if defined $self->worker();
	$self->logDebug("worker", $worker);
	
	for ( my $stagecounter = 0; $stagecounter < @$stages; $stagecounter++ ) {
		$self->logDebug("stagecounter", $stagecounter);
		my $stage = $$stages[$stagecounter];
		if ( $stagecounter != 0 ) {
			my $ancestor = $stage->getAncestor();
			$self->logDebug("ancestor", $ancestor);
			my $status = $stage->getStatus();
			$self->logDebug("status", $status);
			next if $status eq "skip"
		}

		my $stage_number = $stage->appnumber();
		my $stage_name = $stage->appname();
		
		my $username	=	$stage->username();
		my $projectname		=	$stage->projectname();
		my $workflowname	=	$stage->workflowname();
		
		my $mysqltime	=	$self->getMysqlTime();
		$self->logDebug("mysqltime", $mysqltime);
		$stage->queued($mysqltime);
		$stage->started($mysqltime);
		
		#### CLEAR STDOUT/STDERR FILES
		my $stdoutfile	=	$stage->stdoutfile();
		`rm -fr $stdoutfile` if -f $stdoutfile;
		my $stderrfile	=	$stage->stderrfile();
		`rm -fr $stderrfile` if -f $stderrfile;
		
		#### REPORT STARTING STAGE
		$self->bigDisplayBegin("'$projectname.$workflowname' stage $stage_number $stage_name status: RUNNING");
		
		$stage->initialiseRunTimes($mysqltime);

		#### SET STATUS TO running
		$stage->setStatus('running');

		#### SET QUEUE NAME FOR STAGE
		$stage->setStageQueue( $stage->queue() );

		#### NOTIFY STATUS
		if ( $worker ) {
			$self->updateJobStatus($stage, "started");
		}
		else {
			my $data = $self->_getStatus($username, $projectname, $workflowname);
			$self->logDebug("DOING notifyStatus(data)");
			# $self->notifyStatus($data) if defined $exchange and $exchange eq "true";
		}
		
		####  RUN STAGE
		$self->logDebug("Running stage $stage_number", $stage_name);	
		my ($exitcode) = $stage->run($dryrun);
		$self->logDebug("Stage $stage_number-$stage_name exitcode", $exitcode);

		#### STOP IF THIS STAGE DIDN'T COMPLETE SUCCESSFULLY
		#### ALL APPLICATIONS MUST RETURN '0' FOR SUCCESS)
		if ( defined $exitcode and $exitcode == 0 ) {
			$self->logDebug("Stage $stage_number: '$stage_name' completed successfully");
			$stage->setStatus('completed');
			$self->bigDisplayEnd("'$projectname.$workflowname' stage $stage_number $stage_name status: COMPLETED");
			
			#### NOTIFY STATUS
			my $status	=	"completed";
			if ( $worker ) {
				$self->logDebug("DOING self->updateJobStatus: $status");
				$self->updateJobStatus($stage, $status);
			}
			else {
				my $data = $self->_getStatus($username, $projectname, $workflowname);
				# $self->notifyStatus($data) if defined $exchange and $exchange eq "true";
			}
		}
		else {
			$stage->setStatus('error');
			$self->bigDisplayEnd("'$projectname.$workflowname' stage $stage_number $stage_name status: ERROR");
			#### NOTIFY ERROR
			if ( $worker ) {
				$self->updateJobStatus($stage, "exitcode: $exitcode");
			}
			else {
				my $data = $self->_getStatus($username, $projectname, $workflowname);
				# $self->notifyError($data, "Workflow '$projectname.$workflowname' stage #$stage_number '$stage_name' failed with exitcode: $exitcode") if defined $exchange and $exchange eq "true";
			}
			
			$self->logDebug("Exiting runStages");
			return 0;
		}

		#### SET SUCCESSOR IF PRESENT
		my $successor	=	$stage->getSuccessor();
		$self->logDebug("successor", $successor);
		$stagecounter = $successor - 2 if defined $successor and $successor ne "";
		$self->logDebug("stagecounter", $stagecounter);	
	}   
	
	return 1;
}

method setStages ($username, $cluster, $data, $projectname, $workflowname, $workflownumber, $samplehash, $scheduler, $queue) {
	$self->logGroup("Engine::Cluster::Workflow::setStages");
	$self->logDebug("username", $username);
	$self->logDebug("cluster", $cluster);
	$self->logDebug("projectname", $projectname);
	$self->logDebug("workflowname", $workflowname);
	$self->logDebug("scheduler", $scheduler);
	
	# #### GET SLOTS (NUMBER OF CPUS ALLOCATED TO CLUSTER JOB)
	# my $slots	=	undef;
	# if ( defined $scheduler and $scheduler eq "sge" ) {
	# 	$slots = $self->getSlots($username, $cluster);
	# }
	# $self->logDebug("slots", $slots);
	
	#### SET STAGES
	my $stages = $self->table()->getStagesByWorkflow($data);
	$self->logDebug("# stages", scalar(@$stages) );

	#### VERIFY THAT PREVIOUS STAGE HAS STATUS completed
	my $force = $self->force();
	$self->logDebug("force", $force);
	my $previouscompleted = $self->checkPrevious($stages, $data);
	$self->logDebug("previouscompleted", $previouscompleted);
	return [] if not $previouscompleted and not $force;

	#### GET STAGE PARAMETERS FOR THESE STAGES
	$stages = $self->setStageParameters($stages, $data);
	
	#### SET START AND STOP
	my ($start, $stop) = $self->setStartStop($stages, $data);
	$self->logDebug("start", $start);
	$self->logDebug("stop", $stop);

	if ( not defined $start or not defined $stop ) {
		print "Skipping stages for workflow: $workflowname\n";
		return [];		
	}
	
	#### GET FILEROOT & USERHOME
	my $fileroot = $self->util()->getFileroot($username);	
	$self->logDebug("fileroot", $fileroot);
	
	my $userhome = $self->util()->getUserhome($username);	
	$self->logDebug("userhome", $userhome);

	#### SET FILE DIRS
	my ($scriptdir, $stdoutdir, $stderrdir) = $self->setFileDirs($fileroot, $projectname, $workflowname);
	$self->logDebug("scriptdir", $scriptdir);
	
	#### WORKFLOW PROCESS ID
	my $workflowpid = $self->workflowpid();
	
	#### SET OUTPUT DIR
	my $outputdir =  "$fileroot/$projectname/$workflowname";

	#### GET ENVIRONMENT VARIABLES
	my $envar = $self->envar();

	#### GET MONITOR
	$self->logDebug("BEFORE monitor = self->updateMonitor()");
	my $monitor	= 	undef;
	$monitor = $self->updateMonitor();
	$self->logDebug("AFTER XXX monitor = self->updateMonitor()");

	#### LOAD STAGE OBJECT FOR EACH STAGE TO BE RUN
	my $stageobjects = [];
	for ( my $counter = $start - 1; $counter < $stop - 1; $counter++ ) {
		my $stage = $$stages[$counter];
		$self->logNote("stage", $stage);
		
		my $stagenumber	=	$stage->{appnumber};
		my $stagename		=	$stage->{appname};
		my $id					=	$samplehash->{samplename};
		my $successor		=	$stage->{successor};
		$self->logDebug("successor", $successor) if defined $successor and $successor ne "";
		
		$stage->{stageparameters} = [] if not defined $stage->{stageparameters};
		
		my $stage_number = $counter + 1;

		$stage->{username}		=  	$username;
		$stage->{cluster}			=  	$cluster;
		$stage->{workflowpid}	=		$workflowpid;
		$stage->{table}				=		$self->table();
		$stage->{conf}				=  	$self->conf();
		$stage->{fileroot}		=  	$fileroot;
		$stage->{userhome}		=  	$userhome;

		#### SET SCHEDULER
		$stage->{scheduler}		=	$scheduler;
		
		#### SET MONITOR
		$stage->{monitor} = $monitor;

		#### SET SGE ENVIRONMENT VARIABLES
		$stage->{envar} = $envar;
		
		#### MAX JOBS
		$stage->{maxjobs}		=	$self->maxjobs();

		# #### SLOTS
		# $stage->{slots}			=	$slots;

		#### QUEUE
		$stage->{queue}			=  	$queue;

		#### SAMPLE HASH
		$stage->{samplehash}	=  	$samplehash;
		$stage->{outputdir}		=  	$outputdir;
		$stage->{qsub}			=  	$self->conf()->getKey("scheduler:QSUB");
		$stage->{qstat}			=  	$self->conf()->getKey("scheduler:QSTAT");

		#### LOG
		$stage->{log} 			=	$self->log();
		$stage->{printlog} 		=	$self->printlog();
		$stage->{logfile} 		=	$self->logfile();

        #### SET SCRIPT, STDOUT AND STDERR FILES
		$stage->{scriptfile} 	=	"$scriptdir/$stagenumber-$stagename.sh";
		$stage->{stdoutfile} 	=	"$stdoutdir/$stagenumber-$stagename.stdout";
		$stage->{stderrfile} 	= 	"$stderrdir/$stagenumber-$stagename.stderr";

		if ( defined $id ) {
			$stage->{scriptfile} 	=	"$scriptdir/$stagenumber-$stagename-$id.sh";
			$stage->{stdoutfile} 	=	"$stdoutdir/$stagenumber-$stagename-$id.stdout";
			$stage->{stderrfile} 	= 	"$stderrdir/$stagenumber-$stagename-$id.stderr";
		}

		my $stageobject = Engine::Cluster::Stage->new($stage);

		#### NEAT PRINT STAGE
		#$stageobject->toString();

		push @$stageobjects, $stageobject;
	}

	#### SET self->stages()
	$self->stages($stageobjects);
	$self->logDebug("final no. stageobjects", scalar(@$stageobjects));
	
	$self->logGroupEnd("Engine::Workflow::setStages");

	return $stageobjects;
}

# method getSlots ($username, $cluster) {
# 	$self->logCaller("");

# 	return if not defined $username;
# 	return if not defined $cluster;
	
# 	$self->logDebug("username", $username);
# 	$self->logDebug("cluster", $cluster);
	
# 	#### SET INSTANCETYPE
# 	my $clusterobject = $self->getCluster($username, $cluster);
# 	$self->logDebug("clusterobject", $clusterobject);
# 	my $instancetype = $clusterobject->{instancetype};
# 	$self->logDebug("instancetype", $instancetype);
# 	$self->instancetype($instancetype);

# 	$self->logDebug("DOING self->setSlotNumber");
# 	my $slots = $self->setSlotNumber($instancetype);
# 	$slots = 1 if not defined $slots;
# 	$self->logDebug("slots", $slots);

# 	return $slots;	
# }


method updateJobStatus ($stage, $status) {
	#$self->logDebug("status", $status);
	
	#### FLUSH
	$| = 1;
	
	$self->logDebug("stage", $stage->name());

	#### POPULATE FIELDS
	my $data	=	{};
	my $fields	=	$self->getStageFields();
	foreach my $field ( @$fields ) {
		$data->{$field}	=	$stage->$field();
	}

	#### SET QUEUE IF NOT DEFINED
	my $queue		=	"update.job.status";
	$self->logDebug("queue", $queue);
	$data->{queue}	=	$queue;
	
	#### SAMPLE HASH
	my $samplehash		=	$self->samplehash();
	#$self->logDebug("samplehash", $samplehash);
	my $sample			=	$self->sample();
	#$self->logDebug("sample", $sample);
	$data->{sample}		=	$sample;
	
	#### TIME
	$data->{time}		=	$self->getMysqlTime();
	#$self->logDebug("after time", $data);
	
	#### MODE
	$data->{mode}		=	"updateJobStatus";
	
	#### ADD stage... TO NAME AND NUMBER
	$data->{stage}		=	$stage->name();
	$data->{stagenumber}=	$stage->number();

	#### ADD ANCILLARY DATA
	$data->{status}		=	$status;	
	$data->{host}		=	$self->getHostName();
	$data->{ipaddress}	=	$self->getIpAddress();
	#$self->logDebug("after host", $data);

	#### ADD STDOUT AND STDERR
	my $stdout 			=	"";
	my $stderr			=	"";
	$stdout				=	$self->getFileContents($stage->stdoutfile()) if -f $stage->stdoutfile();
	$stderr				=	$self->getFileContents($stage->stderrfile()) if -f $stage->stderrfile();
	$data->{stderr}		=	$stderr;
	$data->{stdout}		=	$stdout;
	
	#### SEND TOPIC	
	$self->logDebug("DOING self->worker->sendTask(data)");
	my $queuename = "update.job.status";
	$self->worker()->sendTask($queuename, $data);
	$self->logDebug("AFTER self->worker->sendTask(data)");
}

method getIpAddress {
	my $ipaddress	=	`facter ipaddress`;
	$ipaddress		=~ 	s/\s+$//;
	$self->logDebug("ipaddress", $ipaddress);
	
	return $ipaddress;
}

method getHostName {
	my $facter		=	`which facter`;
	$facter			=~	s/\s+$//;
	#$self->logDebug("facter", $facter);
	my $hostname	=	`$facter hostname`;	
	$hostname		=~ 	s/\s+$//;
	#$self->logDebug("hostname", $hostname);

	return $hostname;	
}


### QUEUE MONITOR
method setMonitor {
	my $scheduler	=	$self->scheduler();
	$self->logCaller("scheduler", $scheduler);
	
	return if not $scheduler eq "sge";

	my $monitor = undef;	
	# my $monitor = Engine::Cluster::Monitor::SGE->new({
	# 	conf					=>	$self->conf(),
	# 	# whoami				=>	$self->whoami(),
	# 	# pid						=>	$self->workflowpid(),
	# 	table		   		=>	$self->table(),
	# 	# username			=>	$self->username(),
	# 	# projectname		=>	$self->projectname(),
	# 	# workflowname	=>	$self->workflowname(),
	# 	# cluster				=>	$self->cluster(),
	# 	# envar					=>	$self->envar(),

	# 	# logfile				=>	$self->logfile(),
	# 	# log						=>	$self->log(),
	# 	# printlog			=>	$self->printlog()
	# });
	$self->logDebug("monitor", $monitor);

	# 
	$self->monitor($monitor);
}

method updateMonitor {
	my $scheduler	=	$self->scheduler();
	$self->logCaller("scheduler", $scheduler);
	
	return if not defined $scheduler or not $scheduler eq "sge";

	# $self->monitor()->load ({
	# 	pid						=>	$self->workflowpid(),
	# 	conf 					=>	$self->conf(),
	# 	whoami				=>	$self->whoami(),
	# 	table					=>	$self->table(),
	# 	username			=>	$self->username(),
	# 	projectname		=>	$self->projectname(),
	# 	workflowname	=>	$self->workflowname(),
	# 	cluster				=>	$self->cluster(),
	# 	envar					=>	$self->envar(),
	# 	logfile				=>	$self->logfile(),
	# 	log						=>	$self->log(),
	# 	printlog			=>	$self->printlog()
	# });

	my $monitor = Engine::Cluster::Monitor::SGE->new({
		conf					=>	$self->conf(),
		whoami				=>	$self->whoami(),
		pid						=>	$self->workflowpid(),
		table		   		=>	$self->table(),
		username			=>	$self->username(),
		projectname		=>	$self->projectname(),
		workflowname	=>	$self->workflowname(),
		cluster				=>	$self->cluster(),
		envar					=>	$self->envar(),

		logfile				=>	$self->logfile(),
		log						=>	$self->log(),
		printlog			=>	$self->printlog()
	});
	$self->logNote("monitor", $monitor);

	# $self->monitor($monitor);

	# return $self->monitor();
	return $monitor;
}

#### STOP WORKFLOW
method stopWorkflow {
    $self->logDebug("");
    
	my $data         =	$self->data();

	#### SET EXECUTE WORKFLOW COMMAND
  my $bindir = $self->conf()->getKey("core:INSTALLDIR") . "/cgi-bin";

  my $username = $data->{username};
  my $projectname = $data->{projectname};
  my $workflowname = $data->{workflowname};
	my $cluster = $data->{cluster};
	my $start = $data->{start};
  $start--;
  $self->logDebug("projectname", $projectname);
  $self->logDebug("start", $start);
  $self->logDebug("workflowname", $workflowname);
  
#### GET ALL STAGES FOR THIS WORKFLOW
  my $query = qq{SELECT * FROM stage
WHERE username ='$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND status='running'
ORDER BY appnumber};
	$self->logDebug("$query");
	my $stages = $self->table()->db()->queryhasharray($query);
	$self->logDebug("stages", $stages);

	#### EXIT IF NO PIDS
	$self->logError("No running stages in $projectname.$workflowname") and return if not defined $stages;

	#### WARNING IF MORE THAN ONE STAGE RETURNED (SHOULD NOT HAPPEN 
	#### AS STAGES ARE EXECUTED CONSECUTIVELY)
	$self->logError("More than one running stage in $projectname.$workflowname. Continuing with stopWorkflow") if scalar(@$stages) > 1;

	my $submit = $$stages[0]->{submit};
	$self->logDebug("submit", $submit);

	my $messages = $self->killClusterJob($stages, $projectname, $workflowname, $username, $cluster);
	
	#### UPDATE STAGE STATUS TO 'stopped'
	my $update_query = qq{UPDATE stage
SET status = 'stopped'
WHERE username = '$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
AND status = 'running'
};
	$self->logDebug("$update_query\n");
	my $success = $self->table()->db()->do($update_query);

	$self->notifyError($data, "Could not update stages for $projectname.$workflowname") if not $success;
	$data->{status}	=	"Updated stages for $projectname.$workflowname";	
	$self->notifyStatus($data);
}


method killStages ( $stages ) {

#### 1. 'qdel' THE JOB IDS OF ANY RUNNING STAGES
#### 2. INCLUDES STAGE PID, App PARENT PID AND App CHILD PID)

  $self->logDebug("stages", $stages);
	my $messages = [];
	foreach my $stage ( @$stages ) {
		my $jobid = $stage->{jobid};
		$self->logDebug("jobid", $jobid);

		my $command = "qdel -j $jobid";
		$self->logDebug("command", $command);
		my $output = `$command`;
		$self->logDebug("output", $output);
	}

	return $messages;
}



}	#### class
