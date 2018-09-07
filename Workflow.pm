use MooseX::Declare;

=head2

	PACKAGE		Engine::Workflow
	
	PURPOSE
	
		THE Workflow OBJECT PERFORMS THE FOLLOWING TASKS:
		
			1. SAVE WORKFLOWS
			
			2. RUN WORKFLOWS
			
			3. PROVIDE WORKFLOW STATUS

	NOTES

		Workflow::executeWorkflow
			|
			|
			|
			|
		Workflow::runStages
				|
				|
				|
				-> 	my $stage = Engine::Stage->new()
					...
					|
					|
					-> $stage->run()
						|
						|
						? DEFINED 'CLUSTER' AND 'SUBMIT'
						|				|
						|				|
						|				YES ->  Engine::Stage::runOnCluster() 
						|
						|
						NO ->  Engine::Stage::runLocally()

=cut

use strict;
use warnings;
use Carp;

class Engine::Workflow with (Util::Logger, Util::Timer) {

#### EXTERNAL MODULES
use Data::Dumper;
use FindBin::Real;
use lib FindBin::Real::Bin() . "/lib";
use TryCatch;

##### INTERNAL MODULES	
use DBase::Factory;
use Conf::Yaml;
use Engine::Stage;
# use StarCluster::Main;  #
use Engine::Instance;     #
use Engine::Cluster::Monitor::SGE; #
# use Virtual::Factory;
use Engine::Envar;
use Table::Main;
use Exchange::Main;

#### BOOLEAN
has 'force'	=> ( isa => 'Bool', is => 'rw', default 	=> 	0 	);
has 'dryrun'		=> 	( isa => 'Bool', is => 'rw'	);

# Integers
has 'workflowpid'	=> 	( isa => 'Int|Undef', is => 'rw', required => 0 );
has 'workflownumber'=>  ( isa => 'Int|Undef', is => 'rw' );
has 'start'     	=>  ( isa => 'Int|Undef', is => 'rw' );
has 'stop'     		=>  ( isa => 'Int|Undef', is => 'rw' );
has 'submit'  		=>  ( isa => 'Int|Undef', is => 'rw' );
has 'validated'		=> 	( isa => 'Int|Undef', is => 'rw', default => 0 );
has 'qmasterport'	=> 	( isa => 'Int', is  => 'rw' );
has 'execdport'		=> 	( isa => 'Int', is  => 'rw' );
has 'maxjobs'			=> 	( isa => 'Int', is => 'rw'	);

# String
has 'sample'     	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'scheduler'	 	=> 	( isa => 'Str|Undef', is => 'rw', default	=>	"local");
has 'random'			=> 	( isa => 'Str|Undef', is => 'rw', required	=> 	0);
has 'configfile'	=> 	( isa => 'Str|Undef', is => 'rw', default => '' );
has 'installdir'	=> 	( isa => 'Str|Undef', is => 'rw', default => '' );
has 'fileroot'		=> 	( isa => 'Str|Undef', is => 'rw', default => '' );
has 'qstat'				=> 	( isa => 'Str|Undef', is => 'rw', default => '' );
has 'queue'				=>  ( isa => 'Str|Undef', is => 'rw', default => 'default' );
has 'cluster'			=>  ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'whoami'  		=>  ( isa => 'Str', is => 'rw', lazy	=>	1, builder => "setWhoami" );
has 'username'  	=>  ( isa => 'Str', is => 'rw' );
has 'password'  	=>  ( isa => 'Str', is => 'rw' );
has 'workflowname'=>  ( isa => 'Str', is => 'rw' );
has 'projectname' =>  ( isa => 'Str', is => 'rw' );
has 'outputdir'		=>  ( isa => 'Str', is => 'rw' );
has 'keypairfile'	=> 	( isa => 'Str|Undef', is  => 'rw', required	=>	0	);
has 'keyfile'			=> 	( isa => 'Str|Undef', is => 'rw'	);
has 'instancetype'=> 	( isa => 'Str|Undef', is  => 'rw', required	=>	0	);
has 'sgeroot'			=> 	( isa => 'Str', is  => 'rw', default => "/opt/sge6"	);
has 'sgecell'			=> 	( isa => 'Str', is  => 'rw', required	=>	0	);
has 'upgradesleep'=> 	( isa => 'Int', is  => 'rw', default	=>	10	);

# Object
has 'data'				=> 	( isa => 'HashRef|Undef', is => 'rw', default => undef );
has 'samplehash'	=> 	( isa => 'HashRef|Undef', is => 'rw', required	=>	0	);
has 'ssh'					=> 	( isa => 'Util::Ssh', is => 'rw', required	=>	0	);
has 'opsinfo'			=> 	( isa => 'Ops::MainInfo', is => 'rw', required	=>	0	);	
has 'jsonparser'	=> 	( isa => 'JSON', is => 'rw', lazy => 1, builder => "setJsonParser" );
has 'json'				=> 	( isa => 'HashRef', is => 'rw', required => 0 );
has 'stages'			=> 	( isa => 'ArrayRef', is => 'rw', required => 0 );
has 'stageobjects'=> 	( isa => 'ArrayRef', is => 'rw', required => 0 );
has 'starcluster'	=> 	( isa => 'StarCluster::Main', is => 'rw', lazy => 1, builder => "setStarCluster" );
has 'head'				=> 	( isa => 'Engine::Instance', is => 'rw', lazy => 1, builder => "setHead" );
has 'master'			=> 	( isa => 'Engine::Instance', is => 'rw', lazy => 1, builder => "setMaster" );
has 'monitor'			=> 	( isa => 'Engine::Cluster::Monitor::SGE|Undef', is => 'rw', lazy => 1, builder => "setMonitor" );
has 'worker'			=> 	( isa => 'Maybe', is => 'rw', required => 0 );
has 'virtual'			=> 	( isa => 'Any', is => 'rw', lazy	=>	1, builder	=>	"setVirtual" );

has 'envarsub'	=> ( isa => 'Maybe', is => 'rw' );
has 'customvars'=>	( isa => 'HashRef', is => 'rw' );

has 'db'			=> 	( 
	is => 'rw', 
	isa => 'Any', 
	# lazy	=>	1,	
	# builder	=>	"setDbObject" 
);

has 'conf'			=> 	( 
	is => 'rw',
	isa => 'Conf::Yaml',
	lazy => 1,
	builder => "setConf" 
);

method setConf {
	my $conf 	= Conf::Yaml->new({
		backup		=>	1,
		log		=>	$self->log(),
		printlog	=>	$self->printlog()
	});
	
	$self->conf($conf);
}

has 'table'		=>	(
	is 			=>	'rw',
	isa 		=>	'Table::Main',
	lazy		=>	1,
	builder	=>	"setTable"
);


method setTable () {
	my $table = Table::Main->new({
		conf			=>	$self->conf(),
		log				=>	$self->log(),
		printlog	=>	$self->printlog()
	});

	$self->table($table);	
}

has 'util'		=>	(
	is 			=>	'rw',
	isa 		=>	'Util::Main',
	lazy		=>	1,
	builder	=>	"setUtil"
);

method setUtil () {
	my $util = Util::Main->new({
		conf			=>	$self->conf(),
		log				=>	$self->log(),
		printlog	=>	$self->printlog()
	});

	$self->util($util);	
}

has 'envar'	=> ( 
	is => 'rw',
	isa => 'Envar',
	lazy => 1,
	builder => "setEnvar" 
);

method setEnvar {
	my $customvars	=	$self->can("customvars") ? $self->customvars() : undef;
	my $envarsub	=	$self->can("envarsub") ? $self->envarsub() : undef;
	$self->logDebug("customvars", $customvars);
	$self->logDebug("envarsub", $envarsub);
	
	my $envar = Envar->new({
		db			=>	$self->table()->db(),
		conf		=>	$self->conf(),
		customvars	=>	$customvars,
		envarsub	=>	$envarsub,
		parent		=>	$self
	});
	
	$self->envar($envar);
}

has 'exchange'		=>	(
	is 			=>	'rw',
	isa 		=>	'Util::Main',
	lazy		=>	1,
	builder	=>	"setExchange"
);

method setExchange () {
	my $exchange = Exchange::Main->new({
		conf			=>	$self->conf(),
		log				=>	$self->log(),
		printlog	=>	$self->printlog()
	});

	$self->exchange($exchange);	
}


#### INITIAL METHODS
method BUILD ($hash) {
}

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
	$self->logError("Engine::Workflow::BUILD    conf->getKey(cluster, QSUB) not defined") if not defined $self->conf()->getKey("cluster:QSUB");
	$self->logError("Engine::Workflow::BUILD    conf->getKey(cluster, QSTAT) not defined") if not defined $self->conf()->getKey("cluster:QSTAT");
}

method setDbObject ( $data ) {
	my $user 	=	$data->{username} || $self->conf()->getKey("database:USER");
	my $database 	= $data->{database} || $self->conf()->getKey("database:DATABASE");
	my $host	= $self->conf()->getKey("database:HOST");
	my $password	= $data->{password} || $self->conf()->getKey("database:PASSWORD");
	my $dbtype = $data->{dbtype} || $self->conf()->getKey("database:DBTYPE");
	my $dbfile = $data->{dbfile} || $self->conf()->getKey("core:INSTALLDIR") . "/" .$self->conf()->getKey("database:DBFILE");
	$self->logDebug("database", $database);
	$self->logDebug("user", $user);
	$self->logDebug("dbtype", $dbtype);
	$self->logDebug("dbfile", $dbfile);

   #### CREATE DB OBJECT USING DBASE FACTORY
    my $db = DBase::Factory->new( $dbtype,
      {
				database		=>	$database,
	      dbuser      =>  $user,
	      dbpassword  =>  $password,
	      dbhost  		=>  $host,
	      dbfile  		=>  $dbfile,
				logfile			=>	$self->logfile(),
				log					=>	2,
				printlog		=>	2
      }
    ) or die "Can't create database object to create database: $database. $!\n";

	$self->db($db);
}


method setUserLogfile ($username, $identifier, $mode) {
	my $installdir = $self->conf()->getKey("core:INSTALLDIR");
	$identifier	=~ s/::/-/g;
	
	return "$installdir/log/$username.$identifier.$mode.log";
}

### EXECUTE PROJECT
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
	$self->logDebug("data", $data);
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
	my $scheduler			=	$self->conf()->getKey("core:SCHEDULER");
	my $force 		=	$self->force() || $data->{force};
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

	#### SET SCHEDULER
	$self->scheduler($scheduler);

	$data = {
		username				=>	$username,
		projectname					=>	$projectname,
		workflowname				=>	$workflowname,
		workflownumber	=> 	$workflownumber,
		start						=>	$start,
		samplehash			=>	$samplehash
	};

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

	#### SET WORKFLOW 'RUNNING'
	$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, "running");

	#### SET STAGES
	$self->logDebug("DOING self->setStages");
	my $stages = $self->setStages($username, $cluster, $data, $projectname, $workflowname, $workflownumber, $samplehash, $scheduler);
	$self->logDebug("no. stages", scalar(@$stages));
	if ( scalar(@$stages) == 0 ) {
		print "Skipping workflow: $workflowname\n";
		return;
	}

	#### NOTIFY RUNNING
	print "Running workflow $projectname.$workflowname\n";
	my $status;

	#### RUN LOCALLY OR ON CLUSTER
	my $success;
	if ( not defined $cluster or not $cluster or $scheduler eq "local" or not defined $scheduler ) {
		$self->logDebug("DOING self->runLocally");
		$success	=	$self->runLocally($stages, $username, $projectname, $workflowname, $workflownumber, $cluster, $dryrun);
	}
	elsif ( $scheduler eq "sge" ) {
			$self->logDebug("DOING self->runSge");
			$success	=	$self->runSge($stages, $username, $projectname, $workflowname, $workflownumber, $cluster);
	}
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

method addQueueSample ($uuid, $status, $data) {
	$self->logDebug("uuid", $uuid);
	$self->logDebug("status", $status);
	$self->logDebug("data", $data);
	
	#### SET STATUS
	$data->{status}	=	$status;
	
	#### SET SAMPLE
	$data->{samplename}	=	$data->{samplehash}->{samplename};
	
	#### SET TIME
	my $time		=	$self->getMysqlTime();
	$data->{time}	=	$time;
	$self->logDebug("data", $data);

	$self->logDebug("BEFORE setDbh    self->table()->db(): " . $self->table()->db());
	$self->setDbh() if not defined $self->table()->db();
	$self->logDebug("AFTER setDbh    self->table()->db(): " . $self->table()->db());
	
	my $table		=	"queuesample";
	my $keys		=	["username", "projectname", "workflow", "sample"];
	
	$self->logDebug("BEFORE addToTable");
	my $success	=	$self->_addToTable($table, $data, $keys);
	$self->logDebug("AFTER addToTable success", $success);
	
	return $success;
}

#### EXECUTE SAMPLE WORKFLOWS IN PARALLEL
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

	#### CREATE QUEUE FOR WORKFLOW
	$self->createQueue($username, $cluster, $projectname, $workflowname, $envar) if defined $scheduler and $scheduler eq "sge";

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

#### RUN STAGES 
method runLocally ($stages, $username, $projectname, $workflowname, $workflownumber, $cluster, $dryrun) {
	$self->logDebug("# stages", scalar(@$stages));

	#### RUN STAGES
	$self->logDebug("BEFORE runStages()\n");
	my $success	=	$self->runStages($stages, $dryrun);
	$self->logDebug("AFTER runStages    success: $success\n");
	
	if ( $success == 0 ) {
		#### SET WORKFLOW STATUS TO 'error'
		$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'error');
	}
	else {
		#### SET WORKFLOW STATUS TO 'completed'
		$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'completed');
	}
	
	return $success;
}

method runSge ($stages, $username, $projectname, $workflowname, $workflownumber, $cluster) {	
#### RUN STAGES ON SUN GRID ENGINE

	my $sgeroot	=	$self->conf()->getKey("cluster:SGEROOT");
	my $celldir	=	"$sgeroot/$workflowname";
	$self->logDebug("celldir", $celldir);
	$self->_newCluster($username, $workflowname) if not -d $celldir;

	#### CREATE UNIQUE QUEUE FOR WORKFLOW
	my $envar = $self->envar($username, $cluster);
	$self->logDebug("envar", $envar);
	$self->createQueue($username, $cluster, $projectname, $workflowname, $envar);

	# #### SET CLUSTER WORKFLOW STATUS TO 'running'
	# $self->updateClusterWorkflow($username, $cluster, $projectname, $workflowname, 'running');
	
	#### SET WORKFLOW STATUS TO 'running'
	$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'running');
	
	### RELOAD DBH
	$self->setDbh();
	
	#### RUN STAGES
	$self->logDebug("BEFORE runStages()\n");
	my $success	=	$self->runStages($stages);
	$self->logDebug("AFTER runStages    success: $success\n");
	
	#### RESET DBH JUST IN CASE
	$self->setDbh();
	
	if ( $success == 0 ) {
		#### SET CLUSTER WORKFLOW STATUS TO 'completed'
		$self->updateClusterWorkflow($username, $cluster, $projectname, $workflowname, 'error');
		
		#### SET WORKFLOW STATUS TO 'completed'
		$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'error');
	}
	else {
		#### SET CLUSTER WORKFLOW STATUS TO 'completed'
		$self->updateClusterWorkflow($username, $cluster, $projectname, $workflowname, 'completed');
	
		#### SET WORKFLOW STATUS TO 'completed'
		$self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, 'completed');
	}
}


method ensureSgeRunning ($username, $cluster, $projectname, $workflowname) {
	$self->logDebug("");
	
	#### RESET DBH JUST IN CASE
	$self->setDbh();
	
	#### CHECK SGE IS RUNNING ON MASTER THEN HEADNODE
	$self->logDebug("DOING self->checkSge($username, $cluster)");
	my $isrunning = $self->checkSge($username, $cluster);
	$self->logDebug("isrunning", $isrunning);
	
	#### RESET DBH IF NOT DEFINED
	$self->logDebug("DOING self->setDbh()");
	$self->setDbh();

	if ( $isrunning ) {
		#### UPDATE CLUSTER STATUS TO 'running'
		$self->updateClusterStatus($username, $cluster, 'SGE running');
		
		return 1;
	}
	else {
		#### SET CLUSTER STATUS TO 'error'
		$self->updateClusterStatus($username, $cluster, 'SGE error');

		$self->logDebug("Failed to start SGE");
		
		return 0;
	}
}


#### STAGES
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
		if ( $exitcode == 0 ) {
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

method setStages ($username, $cluster, $data, $projectname, $workflowname, $workflownumber, $samplehash, $scheduler) {
	$self->logGroup("Engine::Workflow::setStages");
	$self->logDebug("username", $username);
	$self->logDebug("cluster", $cluster);
	$self->logDebug("projectname", $projectname);
	$self->logDebug("workflowname", $workflowname);
	$self->logDebug("scheduler", $scheduler);
	
	#### GET SLOTS (NUMBER OF CPUS ALLOCATED TO CLUSTER JOB)
	my $slots	=	undef;
	if ( defined $scheduler and $scheduler eq "sge" ) {
		$slots = $self->getSlots($username, $cluster);
	}
	$self->logDebug("slots", $slots);
	
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

	#### CLUSTER, QUEUE AND QUEUE OPTIONS
	my $queue = $self->util()->queueName($username, $projectname, $workflowname);
	#my $queue_options = $self->data()->{queue_options};
	
	#### SET OUTPUT DIR
	my $outputdir =  "$fileroot/$projectname/$workflowname";

	#### GET ENVIRONMENT VARIABLES
	my $envar = $self->envar();

	#### GET MONITOR
	$self->logDebug("BEFORE monitor = self->updateMonitor()");
	my $monitor	= 	undef;
	#$monitor = $self->updateMonitor() if $scheduler eq "sge" or $scheduler eq "starcluster";
	$monitor = $self->updateMonitor();
	$self->logDebug("AFTER XXX monitor = self->updateMonitor()");

	#### LOAD STAGE OBJECT FOR EACH STAGE TO BE RUN
	my $stageobjects = [];
	for ( my $counter = $start - 1; $counter < $stop - 1; $counter++ ) {
		my $stage = $$stages[$counter];
		$self->logDebug("stage", $stage);
		
		my $stagenumber	=	$stage->{appnumber};
		my $stagename		=	$stage->{appname};
		my $id					=	$samplehash->{samplename};
		my $successor		=	$stage->{successor};
		$self->logDebug("successor", $successor) if defined $successor and $successor ne "";
		
		#### STOP IF NO STAGE PARAMETERS
		$self->logDebug("stageparameters not defined for stage $counter $stage->{name}") and last if not defined $stage->{stageparameters};
		
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

		#### SLOTS
		$stage->{slots}			=	$slots;

		#### QUEUE
		$stage->{queue}			=  	$queue;

		#### SAMPLE HASH
		$stage->{samplehash}	=  	$samplehash;
		$stage->{outputdir}		=  	$outputdir;
		$stage->{qsub}			=  	$self->conf()->getKey("cluster:QSUB");
		$stage->{qstat}			=  	$self->conf()->getKey("cluster:QSTAT");

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

		my $stageobject = Engine::Stage->new($stage);

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

method getSlots ($username, $cluster) {
	$self->logCaller("");

	return if not defined $username;
	return if not defined $cluster;
	
	$self->logDebug("username", $username);
	$self->logDebug("cluster", $cluster);
	
	#### SET INSTANCETYPE
	my $clusterobject = $self->getCluster($username, $cluster);
	$self->logDebug("clusterobject", $clusterobject);
	my $instancetype = $clusterobject->{instancetype};
	$self->logDebug("instancetype", $instancetype);
	$self->instancetype($instancetype);

	$self->logDebug("DOING self->setSlotNumber");
	my $slots = $self->setSlotNumber($instancetype);
	$slots = 1 if not defined $slots;
	$self->logDebug("slots", $slots);

	return $slots;	
}

method setFileDirs ($fileroot, $projectname, $workflowname) {
	$self->logDebug("fileroot", $fileroot);
	$self->logDebug("projectname", $projectname);
	$self->logDebug("workflowname", $workflowname);
	my $scriptdir = $self->util()->createDir("$fileroot/$projectname/$workflowname/script");
	my $stdoutdir = $self->util()->createDir("$fileroot/$projectname/$workflowname/stdout");
	my $stderrdir = $self->util()->createDir("$fileroot/$projectname/$workflowname/stdout");
	$self->logDebug("scriptdir", $scriptdir);

	#### CREATE DIRS	
	`mkdir -p $scriptdir` if not -d $scriptdir;
	`mkdir -p $stdoutdir` if not -d $stdoutdir;
	`mkdir -p $stderrdir` if not -d $stderrdir;
	$self->logError("Cannot create directory scriptdir: $scriptdir") and return undef if not -d $scriptdir;
	$self->logError("Cannot create directory stdoutdir: $stdoutdir") and return undef if not -d $stdoutdir;
	$self->logError("Cannot create directory stderrdir: $stderrdir") and return undef if not -d $stderrdir;		

	return $scriptdir, $stdoutdir, $stderrdir;
}

method getStageApp ($stage) {
	$self->logDebug("stage", $stage);
	
	my $appname		=	$stage->name();
	my $installdir	=	$stage->installdir();
	my $version		=	$stage->version();
	
	my $query	=	qq{SELECT * FROM package
WHERE appname='$stage->{appname}'
AND installdir='$stage->{installdir}'
AND version='$stage->{version}'
};
	$self->logDebug("query", $query);
	my $app	=	$self->table()->db()->query($query);
	$self->logDebug("app", $app);

	return $app;
}
method getStageFields {
	return [
		'username',
		'projectname',
		'workflowname',
		'workflownumber',
		'samplehash',
		'appname',
		'appnumber',
		'apptype',
		'location',
		'installdir',
		'version',
		'queued',
		'started',
		'completed'
	];
}

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

method getWorkflowStages ($json) {
	my $username = $json->{username};
    my $projectname = $json->{projectname};
    my $workflowname = $json->{workflowname};

	#### CHECK INPUTS
    $self->logError("Engine::Workflow::getWorkflowStages    username not defined") if not defined $username;
    $self->logError("Engine::Workflow::getWorkflowStages    projectname not defined") if not defined $projectname;
    $self->logError("Engine::Workflow::getWorkflowStages    workflowname not defined") if not defined $workflowname;

	#### GET ALL STAGES FOR THIS WORKFLOW
    my $query = qq{SELECT * FROM stage
WHERE username ='$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
ORDER BY appnumber};
    $self->logNote("$$ $query");
    my $stages = $self->table()->db()->queryhasharray($query);
	$self->logError("stages not defined for username: $username") and return if not defined $stages;	

	$self->logNote("$$ stages:");
	foreach my $stage ( @$stages )
	{
		my $stage_number = $stage->number();
		my $stage_name = $stage->name();
		my $stage_submit = $stage->submit();
		print "Engine::Workflow::runStages    stage $stage_number: $stage_name [submit: $stage_submit]";
	}

	return $stages;
}

method checkPrevious ($stages, $data) {
	#### IF NOT STARTING AT BEGINNING, CHECK IF PREVIOUS STAGE COMPLETED SUCCESSFULLY
	
	my $start = $data->{start};
    $start--;	
	$self->logDebug("start", $start);
	return 1 if $start <= 0;

	my $stage_number = $start - 1;
	$$stages[$stage_number]->{appname} = $$stages[$stage_number]->{name};
	$$stages[$stage_number]->{appnumber} = $$stages[$stage_number]->{number};
	my $keys = ["username", "projectname", "workflowname", "appname", "appnumber"];
	my $where = $self->table()->db()->where($$stages[$stage_number], $keys);
	my $query = qq{SELECT status FROM stage $where};
	$self->logDebug("query", $query);
	my $status = $self->table()->db()->query($query);
	
	return 1 if not defined $status or not $status;
	$self->logError("previous stage not completed: $stage_number") and return 0 if $status ne "completed";
	return 1;
}

method setStageParameters ($stages, $data) {
	#### GET THE PARAMETERS FOR THE STAGES WE WANT TO RUN
	#$self->logDebug("stages", $stages);
	#$self->logDebug("data", $data);
	
	my $start = $data->{start} || 1;
    $start--;
	for ( my $i = $start; $i < @$stages; $i++ ) {
		my $keys = ["username", "projectname", "workflowname", "appname", "appnumber"];
		my $where = $self->table()->db()->where($$stages[$i], $keys);
		my $query = qq{SELECT * FROM stageparameter
$where AND paramtype='input'
ORDER BY ordinal};
		#$self->logDebug("query", $query);

		my $stageparameters = $self->table()->db()->queryhasharray($query);
		$self->logNote("stageparameters", $stageparameters);
		$$stages[$i]->{stageparameters} = $stageparameters;
	}
	
	return $stages;
}

method setStartStop ($stages, $json) {
	$self->logDebug("# stages", scalar(@$stages));
	$self->logDebug("stages is empty") and return if not scalar(@$stages);

	my $start = $self->start();
	my $stop = $self->stop();
	$self->logDebug("self->start", $self->start());
	$self->logDebug("self->stop", $self->stop());

	#### SET DEFAULTS	
	$start	=	1 if not defined $start;
	$stop 	=	scalar(@$stages) + 1 if not defined $stop;
	$self->logDebug("start", $start);
	$self->logDebug("stop", $stop);

	$self->logDebug("start not defined") and return if not defined $start;
	$self->logDebug("start is non-numeric: $start") and return if $start !~ /^\d+$/;

	if ( $start > @$stages ) {
		print "Stage start ($start) is greater than the number of stages: " . scalar(@$stages) . "\n";
		$self->logDebug("Stage start ($start) is greater than the number of stages");
		return;

	}

	if ( defined $stop and $stop ne '' ) {
		if ( $stop !~ /^\d+$/ ) {
			$self->logDebug("Stage stop is non-numeric: $stop");
			return;
		}
		elsif ( $stop > scalar(@$stages) + 1 ) {
			print "Stage stop ($stop) is greater than total stages: " . scalar(@$stages) . "\n";
			$self->logDebug("Stage stop ($stop) is greater than total stages: " . scalar(@$stages) );
			return;
		}
	}
	else {
		$stop = scalar(@$stages) + 1;
	}
	
	if ( $start > $stop ) {
		print "Stage start ($start) is greater than stage stop ($stop)\n";
		$self->logDebug("start ($start) is greater than stop ($stop)");
		return;
	}

	$self->logNote("$$ Setting start: $start");	
	$self->logNote("$$ Setting stop: $stop");
	
	$self->start($start);
	$self->stop($stop);
	
	return ($start, $stop);
}

### QUEUE MONITOR
method setMonitor {
	my $scheduler	=	$self->scheduler();
	$self->logCaller("scheduler", $scheduler);
	
	return if not $scheduler eq "sge";
	
	my $monitor = Engine::Cluster::Monitor::SGE->new({
		conf		=>	$self->conf(),
		whoami		=>	$self->whoami(),
		pid			=>	$self->workflowpid(),
		db			=>	$self->table()->db(),
		username	=>	$self->username(),
		projectname		=>	$self->projectname(),
		workflowname	=>	$self->workflowname(),
		cluster		=>	$self->cluster(),
		envar		=>	$self->envar(),

		logfile		=>	$self->logfile(),
		log		=>	$self->log(),
		printlog	=>	$self->printlog()
	});
	
	$self->monitor($monitor);
}
method updateMonitor {
	my $scheduler	=	$self->scheduler();
	$self->logCaller("scheduler", $scheduler);
	
	return if not defined $scheduler or not $scheduler eq "sge";

	$self->monitor()->load ({
		pid			=>	$self->workflowpid(),
		conf 		=>	$self->conf(),
		whoami		=>	$self->whoami(),
		db			=>	$self->table()->db(),
		username	=>	$self->username(),
		projectname		=>	$self->projectname(),
		workflowname	=>	$self->workflowname(),
		cluster		=>	$self->cluster(),
		envar		=>	$self->envar(),
		logfile		=>	$self->logfile(),
		log			=>	$self->log(),
		printlog	=>	$self->printlog()
	});

	return $self->monitor();
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

	my $messages;
	if ( defined $submit and $submit ) {
		$self->logDebug("Doing killClusterJob(stages)");
		$messages = $self->killClusterJob($projectname, $workflowname, $username, $cluster, $stages);
	}
	else {
		$self->logDebug("Doing killLocalJob(stages)");
		$messages = $self->killLocalJob($stages);
	}
	
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

method killLocalJob ($stages) {
#### 1. 'kill -9' THE PROCESS IDS OF ANY RUNNING STAGE OF THE WORKFLOW
#### 2. INCLUDES STAGE PID, App PARENT PID AND App CHILD PID)

    $self->logDebug("stages", $stages);
	my $messages = [];
	foreach my $stage ( @$stages )
	{
		#### OTHERWISE, KILL ALL PIDS
		push @$messages, $self->killPid($stage->{childpid}) if defined $stage->{childpid};
		push @$messages, $self->killPid($stage->{parentpid}) if defined $stage->{parentpid};
		push @$messages, $self->killPid($stage->{stagepid}) if defined $stage->{stagepid};
		push @$messages, $self->killPid($stage->{workflowpid}) if defined $stage->{workflowpid};
	}

	return $messages;
}

#### GET STATUS
method _getStatus ($username, $projectname, $workflowname) {
=head2

SUBROUTINE	_getStatus

PURPOSE

 1. GET STATUS FROM stage TABLE
 2. UPDATE stage TABLE WITH JOB STATUS FROM QSTAT IF CLUSTER IS RUNNING

OUTPUT

	{
		stagestatus 	=> 	{
			projectname		=>	String,
			workflowname	=>	String,
			stages		=>	HashArray,
			status		=>	String
		}
	}

=cut

    #$self->logDebug("username", $username);
    #$self->logDebug("projectname", $projectname);
    #$self->logDebug("workflowname", $workflowname);

	#### GET STAGES FROM stage TABLE
	my $now = $self->table()->db()->now();
	$self->logDebug("now", $now);

	my $datetime = $self->table()->db()->query("SELECT $now");
	$self->logDebug("datetime", $datetime);
  my $query = qq{SELECT *
FROM stage
WHERE username ='$username'
AND projectname = '$projectname'
AND workflowname = '$workflowname'
ORDER BY appnumber
};
	#$self->logDebug("query", $query);
  my $stages = $self->table()->db()->queryhasharray($query);
  for my $stage ( @$stages ) {
  	$stage->{now} = $datetime;
  }
	$self->logDebug("# stages", scalar(@$stages)) if defined $stages;
	# $self->logDebug("stages", $stages);

	##### PRINT STAGES
	#$self->printStages($stages);
	
	#### QUIT IF stages NOT DEFINED
	$self->notifyError({}, "No stages with run status for username: $username, projectname: $projectname, workflowname: $workflowname") and return if not defined $stages;

  #### PRINT stages AND RETURN IF CLUSTER IS NOT DEFINED
	return $self->_getStatusLocal($username, $projectname, $workflowname, $stages);
}

method printStages ($stages) {
	foreach my $stage ( @$stages ) {
		$self->printStage($stage);
	}
}

method printStage ( $data ) {
	my $fields = [ 'owner', 'appname', 'appnumber', 'apptype', 'location', 'submit', 'executor', 'prescript', 'cluster', 'description', 'notes' ];
	print "STAGE $data->{number}\n";
	foreach my $field ( @$fields ) {
		print "\t$field: $data->{$field}\n" if defined $data->{$field} and $data->{$field} ne "";
	}
}

method getWorkflowStatus ($username, $projectname, $workflowname) {
	$self->logDebug("workflowname", $workflowname);

	my $object = $self->table()->getWorkflow($username, $projectname, $workflowname);
	$self->logDebug("object", $object);
	return if not defined $object;
	
	return $object->{status};
}

method _getStatusLocal ($username, $projectname, $workflowname, $stages) {
#### PRINT stages AND RETURN IF CLUSTER IS NOT DEFINED
#### I.E., JOB WAS SUBMITTED LOCALLY. 
#### NB: THE stage TABLE SHOULD BE UPDATED BY THE PROCESS ON EXIT.

	$self->logDebug("username", $username);
	
	my $workflowobject = $self->table()->getWorkflow($username, $projectname, $workflowname);
	#$self->logDebug("workflowobject", $workflowobject);
	my $status = $workflowobject->{status} || '';
	my $stagestatus 	= 	{
		projectname		=>	$projectname,
		workflowname	=>	$workflowname,
		stages		=>	$stages,
		status		=>	$status
	};
	#$self->logDebug("stagestatus", $stagestatus);
	
	return $stagestatus;	
}

method updateStageStatus($monitor, $stages) {
#### UPDATE stage TABLE WITH JOB STATUS FROM QSTAT
	my $statusHash = $monitor->statusHash();
	$self->logDebug("statusHash", $statusHash);	
	foreach my $stage ( @$stages ) {
		my $stagejobid = $stage->{stagejobid};
		next if not defined $stagejobid or not $stagejobid;
		$self->logDebug("pid", $stagejobid);

		#### GET STATUS
		my $status;
		if ( defined $statusHash )
		{
			$status = $statusHash->{$stagejobid};
			next if not defined $status;
			$self->logDebug("status", $status);

			#### SET TIME ENTRY TO BE UPDATED
			my $timeentry = "queued";
			$timeentry = "started" if defined $status and $status eq "running";

			$timeentry = "completed" if not defined $status;
			$status = "completed" if not defined $status;
		
			#### UPDATE THE STAGE ENTRY IF THE STATUS HAS CHANGED
			if ( $status ne $stage->{status} )
			{
				my $now = $self->table()->db()->now();
				my $query = qq{UPDATE stage
SET status='$status',
$timeentry=$now
WHERE username ='$stage->{username}'
AND projectname = '$stage->{projectname}'
AND workflowname = '$stage->{workflowname}'
AND number='$stage->{number}'};
				$self->logDebug("query", $query);
				my $result = $self->table()->db()->do($query);
				$self->logDebug("status update result", $result);
			}
		}
	}	
}

#### UPDATE
method updateWorkflowStatus ($username, $cluster, $projectname, $workflowname, $status) {
	$self->logDebug("status", $status);

#   #### DEBUG 
#   # my $query = "SELECT * FROM workflow";

#   my $query = qq{UPDATE workflow SET status = 'running'
#  WHERE username = 'testuser'
# AND projectname = 'Project1'
# AND name = 'Workflow1'};
#   $self->logDebug("query", $query);
#   # my $results = $self->table()->db()->queryarray($query);
#   # my $results = $self->table()->db()->queryarray($query);
#   my $results = $self->table()->db()->queryarray($query);
#   $self->logDebug("results", $results);

	my $table ="workflow";
	my $hash = {
		username			=>	$username,
		cluster				=>	$cluster,
		projectname		=>	$projectname,
		workflowname	=>	$workflowname,
		status				=>	$status,
	};
	$self->logDebug("hash", $hash);
	my $required_fields = ["username", "projectname", "workflowname"];
	my $set_hash = {
		status		=>	$status
	};
	my $set_fields = ["status"];
	$self->logDebug("BEFORE _updateTable   hash", $hash);
	$self->logDebug("self->db", $self->table()->db());

	# my $success = $self->table()->db()->_updateTable($table, $hash, $required_fields, $set_hash, $set_fields);
	my $success = $self->table()->db()->_updateTable($table, $hash, $required_fields, $set_hash, $set_fields);
	$self->logDebug("success", $success);
	
	return $success;
}

method setWorkflowStatus ($status, $data) {
	$self->logDebug("status", $status);
	$self->logDebug("data", $data);
	
	my $query = qq{UPDATE workflow
SET status = '$status'
WHERE username = '$data->{username}'
AND projectname = '$data->{projectname}'
AND workflowname = '$data->{workflowname}'
AND workflownumber = $data->{workflownumber}};
	$self->logDebug("$query");

	my $success = $self->table()->db()->do($query);
	if ( not $success ) {
		$self->logError("Can't update workflow $data->{workflowname} (projectname: $data->{projectname}) with status: $status");
		return 0;
	}
	
	return 1;
}

#### SET WHOAMI
method setWhoami {
	my $whoami	=	`whoami`;
	$whoami		=~	s/\s+$//;
	$self->logDebug("whoami", $whoami);
	
	return $whoami;
}

method bigDisplayBegin ($message) {
	print qq{
##########################################################################
#### $message
####
};
	
}

method bigDisplayEnd ($message) {
	print qq{
####
#### $message
##########################################################################
};
	
}

#
}	#### Engine::Workflow
