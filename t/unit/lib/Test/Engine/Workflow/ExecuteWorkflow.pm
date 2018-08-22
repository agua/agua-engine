use MooseX::Declare;

class Test::Engine::Workflow::ExecuteWorkflow with Test::Common extends Engine::Workflow {

#### EXTERNAL MODULES
use Test::More;

#### INTERNAL MODULES
use DBase::Factory;
use Test::Table::Main;
use Test::Engine::Cluster::Monitor::SGE;

has 'dumpfile'		=> ( isa => 'Str|Undef', is => 'rw' );
has 'conf'	=> ( isa => 'Conf::Yaml', is => 'rw', lazy => 1, builder => "setConf" );
has 'starcluster'	=> ( isa => 'Test::StarCluster::Main', is => 'rw', lazy => 1, builder => "setStarCluster" );
has 'monitor'	=> (
	is 		=>	'rw',
	isa 	=> 'Test::Engine::Cluster::Monitor::SGE',
	default	=>	sub { Test::Engine::Cluster::Monitor::SGE->new({});	}
);

has custom_fields => (
    traits     => [qw( Hash )],
    isa        => 'HashRef',
    builder    => '_build_custom_fields',
    handles    => {
        custom_field         => 'accessor',
        has_custom_field     => 'exists',
        custom_fields        => 'keys',
        has_custom_fields    => 'count',
        delete_custom_field  => 'delete',
    },
);

has 'table' => ( 
  is => 'rw',
  isa => 'Test::Table::Main',
  lazy => 1,
  builder => "setTestTable" 
);

method setTestTable () {
  my $table = Test::Table::Main->new({
    conf      =>  $self->conf(),
    log       =>  $self->log(),
    printlog  =>  $self->printlog()
  });

  $self->table($table); 
}

sub _build_custom_fields { {} }

#####/////}}}}

method BUILD ( $hash ) {
	print "Test::Engine::Workflow::BUILD\n";
	
	$self->initialise($hash);
}

method initialise ($hash) {
	print "Test::Engine::Workflow::initialise\n";
	if ( $hash ) {
		foreach my $key ( keys %{$hash} ) {
			$self->$key($hash->{$key}) if $self->can($key);
		}
	}
	$self->logDebug("hash", $hash);

  #### SET DATABASE HANDLE
  $self->logDebug("Doing self->setDbObject");
  $hash->{database}    =   $self->conf()->getKey("database:TESTDATABASE");
  $hash->{dbuser}      =   $self->conf()->getKey("database:TESTUSER");
  $hash->{dbpassword}  =   $self->conf()->getKey("database:TESTPASSWORD");
  $hash->{sessionid}   =   $self->conf()->getKey("database:TESTSESSIONID");
  $hash->{dbfile}      =   $self->conf()->getKey("core:INSTALLDIR") . "/" . $self->conf()->getKey("database:TESTDBFILE");

  $self->setDbObject( $hash ) if not defined $self->table()->db();
}

method testStartStop {
    #### SET USERNAME AND CLUSTER
	diag("startStop");

   	#### RESET DATABASE
	$self->setUpTestDatabase();
	$self->setDatabaseHandle();

    #### LOAD TSVFILES
	$self->loadTsvFile("project", "$Bin/inputs/startstop/project.tsv");
	$self->loadTsvFile("workflow", "$Bin/inputs/startstop/workflow.tsv");
	$self->loadTsvFile("stage", "$Bin/inputs/startstop/stage.tsv");
	$self->loadTsvFile("stageparameter", "$Bin/inputs/startstop/stageparameter.tsv");

	my $data = {
		"database"	=> 	"aguatest",
		"username"	=>	"guest",
		"sessionid"	=>	"0000000000.0000.000",
		"sourceid"	=>	"plugins/workflow/RunStatus/Status_0",
		"mode"		=>	"executeWorkflow",
		"module"	=>	"Engine::Workflow",
		"callback"	=>	"handleStatus",
		"sendtype"	=>	"request",
		"cluster"	=>	"",
		"project"	=>	"Project1",
		"workflow"	=>	"Workflow1",
		"workflownumber"	=>	"1",
		"start"		=>	1,
		"stop"		=>	2,-
		"submit"	=>	0
	};
	
	foreach my $key ( keys %$data ) {
		$self->logDebug("loading key", $key);
		if ( $self->can($key) ) {
			$self->$key($data->{$key});		
		}
	}
    $self->logDebug("self->username()", $self->username());
    $self->logDebug("self->cluster()", $self->cluster());
    $self->logDebug("self->project()", $self->project());
    $self->logDebug("self->workflow()", $self->workflow());
    $self->logDebug("self->workflownumber()", $self->workflownumber());
    $self->logDebug("self->start()", $self->start());
    $self->logDebug("self->submit()", $self->submit());
	
	my $message = undef;
	*sendFanout = sub {
		my $self		=	shift;
		my $exchange	=	shift;
		$message 		= 	shift;
	};
	#$self->logDebug("message", $message);

	#### TEST START STARCLUSTER
	$self->logDebug("DOING executeWorkflow()");
	$self->executeWorkflow();	

	my $jsonparser	=	JSON->new();
	my $output = $jsonparser->decode($message);
	#$self->logDebug("output", $output);
	my $stagedata = ${$output->{data}->{stagestatus}->{stages}}[0];
	#$self->logDebug("stagedata", $stagedata);
	my $started = $stagedata->{started};
	my $queued = $stagedata->{queued};
	my $completed = $stagedata->{completed};
	$self->logDebug("started", $started);
	$self->logDebug("queued", $queued);
	$self->logDebug("completed", $completed);

	ok($started eq $queued, "started equals queued");

	#### SIMPLE	SECOND COMPARISON
	my $delay = 3;
	if ( $started =~ /^(\S+)/ eq $queued =~ /^(\S+)/ ) {
		$started =~ /(\d+):(\d+):(\d+)$/;
		my $startedseconds = 3600 * $1 + 60 * $2 + $3;
		$self->logDebug("startedseconds", $startedseconds);
		$completed =~ /(\d+):(\d+):(\d+)$/;
		my $completedseconds = 3600 * $1 + 60 * $2 + $3;
		$self->logDebug("completedseconds", $completedseconds);
		
		ok( ($startedseconds + 5  + $delay)== $completedseconds, "start to completion = 5 seconds");
	}	
}

method recreateOutputDir {
    #### SET DIRS
  my $targetdir = "$Bin/outputs";
  my $command = "rm -fr $targetdir";
  $self->logDebug("command", $command);
  `$command`;
  $command = "mkdir -p $targetdir";
  $self->logDebug("command", $command);
  `$command`;
}

method setWorkflowTestDatabase ( $directory ) {
	$self->logDebug("directory", $directory);
	
 	#### RESET DATABASE
	$self->table()->setUpTestDatabase();
	$self->table()->setDatabaseHandle();
  
  #### LOAD TSVFILES
	my $files = $self->getFiles( $directory );
  $self->logDebug("files", $files);
 #  $self->table()->loadTsvFile("aws", "$targetdir/aws.tsv");
	# $self->table()->loadTsvFile("cluster", "$targetdir/cluster.tsv");
	# $self->table()->loadTsvFile("clusterstatus", "$targetdir/clusterstatus.tsv");
	# $self->table()->loadTsvFile("clustervars", "$targetdir/clustervars.tsv");
	# $self->table()->loadTsvFile("clusterworkflow", "$targetdir/clusterworkflow.tsv");
	# $self->table()->loadTsvFile("stage", "$targetdir/stage.tsv");
	# $self->table()->loadTsvFile("stageparameter", "$targetdir/stageparameter.tsv");
	# $self->table()->loadTsvFile("workflow", "$targetdir/workflow.tsv");
}

method testExecuteWorkflow {
  my $testname = "executeworkflow";
	diag($testname);

  my $data = {
    "username"        =>  "guest",
    "project"         =>  "Project1",
    "workflow"        =>  "Workflow1",
    "workflownumber"  =>  "1",
    "start"           =>  1,
    "stop"            =>  2,
    "submit"          =>  0
  };

	my $username 	=  	$self->conf()->getKey("database:TESTUSER");
	my $project 	=  	"Project1";
	my $workflow 	=  	"Workflow1";
	my $workflownumber 	=  1;
	my $start 		=  	1;
	my $submit 		=  	1;
	
	$self->username($username);
	$self->project($project);
	$self->workflow($workflow);
	$self->workflownumber($workflownumber);
	$self->start($start);
	$self->submit($submit);

  $self->logDebug("username", $username);
  $self->logDebug("project", $project);
  $self->logDebug("workflow", $workflow);
  $self->logDebug("workflownumber", $workflownumber);
  $self->logDebug("start", $start);
  $self->logDebug("submit", $submit);

  #### RESET DATABASE TO EMPTY TABLES
  $self->table()->reloadTestDatabase({});

  #### LOAD DATABASE TABLE DATA
  my $directory = "$Bin/inputs/$testname";
  my $files = $self->getFiles( $directory );
  $self->logDebug("files", $files);
  for my $file ( @$files ) {
    $self->logDebug("file", $file);
    my ($table) = $file =~ /^(.+)\.tsv/;
    $self->logDebug("table", $table);
    $self->table()->loadTsvFile($table, "$directory/$file");
  }

  my $query = qq{UPDATE workflow SET status='runningXXXXX'
WHERE username = 'testuser'
AND project = 'Project1'
AND name = 'Workflow1'};

  $self->logDebug("query", $query);
  my $results = $self->table()->db()->queryarray($query);
  $self->logDebug("results", $results);

	#### OVERRIDE WORKFLOW runStages
	*{runStages} = sub {
		ok(1, "completed updateClusterWorkflow(..., 'running')");
		shift->logDebug("OVERRIDE Engine::Workflow::runStages");
    return 1;
	};

	my $statuses = [];
  *{updateWorkflowStatus} = sub {
    my $self  = shift;
    shift; shift; shift; shift;
    my $status = shift;
    
    $self->logDebug("ADDING status", $status);
    push @$statuses, $status;
  };
  
  #### DEBUG 
  $query = qq{INSERT INTO workflow VALUES ("testuser","Project1","Workflow2",2,"","","","")};
  $self->logDebug("query", $query);
  $results = $self->table()->db()->queryarray($query);
  $self->logDebug("results", $results);

	#### RUN WORKFLOW
	$self->logDebug("DOING executeWorkflow()");
	$self->executeWorkflow( $data );
	
	#### TEST COMPLETED
	ok(1, "completed executeWorkflow()");

	$self->logDebug("statuses", $statuses);
	my $string = join ", ", @$statuses;
	is_deeply($statuses,  ["running","completed","completed"], "status values sequence: $string");
}

#### OVERRIDE
method overrideSequence ($method, $sequence) {
	$self->logDebug("method", $method);
	$self->logDebug("sequence", $sequence);

	#### SET ATTRIBUTES - SEQUENCE AND COUNTER
	my $attribute = "$method-sequence";
	my $counter = "$method-counter";
	$self->custom_field($attribute, $sequence);
	$self->custom_field($counter, 0);

	my $sub = sub {
		my $self	=	shift;
		$self->logDebug("method", $method);

		my $sequence = $self->custom_field($attribute);
		$self->logDebug("sequence", $sequence);

		my $count 	= $self->custom_field($counter);
		my $value 	= 	$$sequence[$count];
		$self->logDebug("counter $count value", $value);
		
		$count++;
		$self->custom_field($counter, $count);
	
		return $value;
	};

	{
		no warnings;
		no strict;
		*{$method} = $sub;
	}
}




}   #### Test::Engine::Workflow
