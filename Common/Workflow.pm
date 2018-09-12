package Engine::Common::Workflow;
use Moose::Role;
use Method::Signatures::Simple;

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

#### EXTERNAL MODULES
use Data::Dumper;
use FindBin::Real;
use lib FindBin::Real::Bin() . "/lib";
# use TryCatch;

##### INTERNAL MODULES	
use Conf::Yaml;
use Engine::Local::Stage;
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
has 'head'				=> 	( isa => 'Engine::Cloud::Instance', is => 'rw', lazy => 1, builder => "setHead" );
has 'master'			=> 	( isa => 'Engine::Cloud::Instance', is => 'rw', lazy => 1, builder => "setMaster" );
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


method setTable {
	$self->logCaller("");

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
	$self->logCaller("");
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
	$self->logError("Engine::Workflow::BUILD    conf->getKey(cluster, QSUB) not defined") if not defined $self->conf()->getKey("scheduler:QSUB");
	$self->logError("Engine::Workflow::BUILD    conf->getKey(cluster, QSTAT) not defined") if not defined $self->conf()->getKey("scheduler:QSTAT");
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

# method setUserLogfile ($username, $identifier, $mode) {
# 	my $installdir = $self->conf()->getKey("core:INSTALLDIR");
# 	$identifier	=~ s/::/-/g;
	
# 	return "$installdir/log/$username.$identifier.$mode.log";
# }

#### EXECUTE PROJECT
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

	# $self->monitor($monitor);
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
	$self->logDebug("monitor", $monitor);

	# $self->monitor($monitor);

	# return $self->monitor();
	return $monitor;
}

#### STOP WORKFLOW
method stopWorkflow ( $username, $projectname, $workflowname, $options ) {
	$self->logDebug("projectname", $projectname);
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

	my $messages = $self->killStages( $stages );
	
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

	return $success;		
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

	my $workflow = $self->table()->getWorkflow($username, $projectname, $workflowname);

	my $status = $workflow->{status} || '';
	my $stagestatus 	= 	{
		projectname		=>	$projectname,
		workflowname	=>	$workflowname,
		stages		=>	$stages,
		status		=>	$status
	};
	
	return $stagestatus;	
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


1;