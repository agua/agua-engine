use MooseX::Declare;

=head2

    PACKAGE        Engine::Workflow
    
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
                ->     my $stage = Engine::Stage->new()
                    ...
                    |
                    |
                    -> $stage->run()
                        |
                        |
                        ? DEFINED 'CLUSTER' AND 'SUBMIT'
                        |                |
                        |                |
                        |                YES ->  Engine::Stage::runOnCluster() 
                        |
                        |
                        NO ->  Engine::Stage::runLocally()

=cut

use strict;
use warnings;
use Carp;

class Engine::Local::Workflow with (Engine::Common::Workflow, 
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
use Engine::Local::Stage;
use Engine::Cloud::Instance;     #
use Engine::Cluster::Monitor::SGE; #
use Engine::Envar;
use Table::Main;
use Exchange::Main;

#### BOOLEAN
has 'force'    => ( isa => 'Bool', is => 'rw', default     =>     0     );
has 'dryrun'        =>     ( isa => 'Bool', is => 'rw'    );

# Integers
has 'workflowpid'    =>     ( isa => 'Int|Undef', is => 'rw', required => 0 );
has 'workflownumber'=>  ( isa => 'Int|Undef', is => 'rw' );
has 'start'         =>  ( isa => 'Int|Undef', is => 'rw' );
has 'stop'             =>  ( isa => 'Int|Undef', is => 'rw' );
has 'submit'          =>  ( isa => 'Int|Undef', is => 'rw' );
has 'validated'        =>     ( isa => 'Int|Undef', is => 'rw', default => 0 );
has 'qmasterport'    =>     ( isa => 'Int', is  => 'rw' );
has 'execdport'        =>     ( isa => 'Int', is  => 'rw' );
has 'maxjobs'            =>     ( isa => 'Int', is => 'rw'    );

# String
has 'sample'         =>  ( isa => 'Str|Undef', is => 'rw' );
has 'scheduler'         =>     ( isa => 'Str|Undef', is => 'rw', default    =>    "local");
has 'random'            =>     ( isa => 'Str|Undef', is => 'rw', required    =>     0);
has 'configfile'    =>     ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'installdir'    =>     ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'fileroot'        =>     ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'whoami'          =>  ( isa => 'Str', is => 'rw', lazy    =>    1, builder => "setWhoami" );
has 'username'      =>  ( isa => 'Str', is => 'rw' );
has 'password'      =>  ( isa => 'Str', is => 'rw' );
has 'workflowname'=>  ( isa => 'Str', is => 'rw' );
has 'projectname' =>  ( isa => 'Str', is => 'rw' );
has 'outputdir'        =>  ( isa => 'Str', is => 'rw' );
has 'upgradesleep'=>     ( isa => 'Int', is  => 'rw', default    =>    10    );

# Object
has 'data'                =>     ( isa => 'HashRef|Undef', is => 'rw', default => undef );
has 'samplehash'    =>     ( isa => 'HashRef|Undef', is => 'rw', required    =>    0    );
has 'ssh'                    =>     ( isa => 'Util::Ssh', is => 'rw', required    =>    0    );
has 'opsinfo'            =>     ( isa => 'Ops::MainInfo', is => 'rw', required    =>    0    );    
has 'jsonparser'    =>     ( isa => 'JSON', is => 'rw', lazy => 1, builder => "setJsonParser" );
has 'json'                =>     ( isa => 'HashRef', is => 'rw', required => 0 );
has 'stages'            =>     ( isa => 'ArrayRef', is => 'rw', required => 0 );
has 'stageobjects'=>     ( isa => 'ArrayRef', is => 'rw', required => 0 );
has 'starcluster'    =>     ( isa => 'StarCluster::Main', is => 'rw', lazy => 1, builder => "setStarCluster" );
has 'head'                =>     ( isa => 'Engine::Cloud::Instance', is => 'rw', lazy => 1, builder => "setHead" );
has 'master'            =>     ( isa => 'Engine::Cloud::Instance', is => 'rw', lazy => 1, builder => "setMaster" );
has 'monitor'            =>     ( isa => 'Engine::Cluster::Monitor::SGE|Undef', is => 'rw', lazy => 1, builder => "setMonitor" );
has 'worker'            =>     ( isa => 'Maybe', is => 'rw', required => 0 );
has 'virtual'            =>     ( isa => 'Any', is => 'rw', lazy    =>    1, builder    =>    "setVirtual" );

has 'envarsub'    => ( isa => 'Maybe', is => 'rw' );
has 'customvars'=>    ( isa => 'HashRef', is => 'rw' );

has 'db'            =>     ( 
    is => 'rw', 
    isa => 'Any', 
    # lazy    =>    1,    
    # builder    =>    "setDbObject" 
);

has 'conf'            =>     ( 
    is => 'rw',
    isa => 'Conf::Yaml',
    lazy => 1,
    builder => "setConf" 
);

method BUILD ($hash) {
}

method initialise ($data) {
    #### SET LOG
    my $username     =    $data->{username};
    my $logfile     =     $data->{logfile};
    my $mode        =    $data->{mode};
    $self->logDebug("logfile", $logfile);
    $self->logDebug("mode", $mode);
    if ( not defined $logfile or not $logfile ) {
        my $identifier     =     "workflow";
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
    my $database         =    $self->database();
    my $username         =    $self->username();
    my $projectname =    $self->projectname();
    $self->logDebug("username", $username);
    $self->logDebug("projectname", $projectname);
    
    my $fields    =    ["username", "projectname"];
    my $data    =    {
        username    =>    $username,
        projectname        =>    $projectname
    };
    my $notdefined = $self->table()->db()->notDefined($data, $fields);
    $self->logError("undefined values: @$notdefined") and return 0 if @$notdefined;
    
    #### RETURN IF RUNNING
    $self->logError("Project is already running: $projectname") and return if $self->projectIsRunning($username, $projectname);
    
    #### GET WORKFLOWS
    my $workflows    =    $self->table()->getWorkflowsByProject({
        username            =>    $username,
        projectname        =>    $projectname
    });
    $self->logDebug("workflows", $workflows);
    
    #### RUN WORKFLOWS
    my $success    =    1;
    foreach my $object ( @$workflows ) {
        $self->logDebug("object", $object);
        $self->username($username);
        $self->projectname($projectname);
        my $workflowname    =    $object->{name};
        $self->logDebug("workflowname", $workflowname);
        $self->workflowname($workflowname);
    
        #### RUN 
        try {
            $success    =    $self->executeWorkflow();        
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
    my $username             =    $data->{username};
    my $cluster             =    $data->{cluster};
    my $projectname     =    $data->{projectname};
    my $workflowname     =    $data->{workflowname};
    my $workflownumber=    $data->{workflownumber};
    my $samplehash         =    $data->{samplehash};
    my $submit                 = $data->{submit};
    my $start                    =    $data->{start};
    my $stop                    =    $data->{stop};
    my $dryrun                =    $data->{dryrun};
    my $scheduler            =    $self->conf()->getKey("core:SCHEDULER");
    my $force         =    $self->force() || $data->{force};
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
        username                =>    $username,
        projectname                    =>    $projectname,
        workflowname                =>    $workflowname,
        workflownumber    =>     $workflownumber,
        start                        =>    $start,
        samplehash            =>    $samplehash
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
    $self->logDebug("DOING self->runLocally");
    my $success    =    $self->runLocally($stages, $username, $projectname, $workflowname, $workflownumber, $cluster, $dryrun);
    $self->logDebug("success", $success);

    #### SET WORKFLOW STATUS
    $status        =    "completed";
    $status        =    "error" if not $success;
    $self->updateWorkflowStatus($username, $cluster, $projectname, $workflowname, $status);

    #### ADD QUEUE SAMPLE
    my $uuid    =    $samplehash->{samplename};
    $self->logDebug("uuid", $uuid);
    if ( defined $uuid ) {
        $success    =    $self->addQueueSample($uuid, $status, $data);
        $self->logDebug("addQueueSample success", $success);    
    }

    #### NOTIFY COMPLETED
    print "Completed workflow $projectname.$workflowname\n";

    $self->logGroupEnd("$$ Engine::Workflow::executeWorkflow    COMPLETED");
}

method addQueueSample ($uuid, $status, $data) {
    $self->logDebug("uuid", $uuid);
    # $self->logDebug("status", $status);
    $self->logDebug("data", $data);
    
    #### SET STATUS
    $data->{status}    =    $status;
    
    #### SET SAMPLE
    $data->{samplename}    =    $data->{samplehash}->{samplename};
    
    #### SET TIME
    my $time        =    $self->getMysqlTime();
    $data->{time}    =    $time;
    $self->logDebug("data", $data);

    $self->logDebug("BEFORE setDbh    self->table()->db(): " . $self->table()->db());
    $self->setDbh() if not defined $self->table()->db();
    $self->logDebug("AFTER setDbh    self->table()->db(): " . $self->table()->db());
    
    my $table        =    "queuesample";
    my $keys        =    ["username", "projectname", "workflow", "sample"];
    
    $self->logDebug("BEFORE addToTable");
    my $success    =    $self->_addToTable($table, $data, $keys);
    $self->logDebug("AFTER addToTable success", $success);
    
    return $success;
}

#### EXECUTE SAMPLE WORKFLOWS IN PARALLEL
method runInParallel ($workflowhash, $sampledata) {
=head2

    SUBROUTINE        executeCluster
    
    PURPOSE
    
        EXECUTE A LIST OF JOBS CONCURRENTLY UP TO A MAX NUMBER
        
        OF CONCURRENT JOBS

=cut

    $self->logCaller("");

    my $username     =    $self->username();
    my $cluster     =    $self->cluster();
    my $projectname     =    $self->projectname();
    my $workflowname     =    $self->workflowname();
    my $workflownumber=    $self->workflownumber();
    my $start         =    $self->start();
    my $submit         =     $self->submit();
    $self->logDebug("submit", $submit);
    $self->logDebug("username", $username);
    $self->logDebug("projectname", $projectname);
    $self->logDebug("workflowname", $workflowname);
    $self->logDebug("workflownumber", $workflownumber);
    $self->logDebug("cluster", $cluster);
    
    print "Running workflow $projectname.$workflowname\n";

    #### GET CLUSTER
    $cluster        =    $self->getClusterByWorkflow($username, $projectname, $workflowname) if $cluster eq "";
    $self->logDebug("cluster", $cluster);    
    $self->logDebug("submit", $submit);    
    
    #### RUN LOCALLY OR ON CLUSTER
    my $scheduler    =    $self->scheduler() || $self->conf()->getKey("core:SCHEDULER");
    $self->logDebug("scheduler", $scheduler);

    #### GET ENVIRONMENT VARIABLES
    my $envar = $self->envar();
    #$self->logDebug("envar", $envar);

    #### CREATE QUEUE FOR WORKFLOW
    $self->createQueue($username, $cluster, $projectname, $workflowname, $envar) if defined $scheduler and $scheduler eq "sge";

    #### GET STAGES
    my $samplehash    =    undef;
    my $stages    =    $self->setStages($username, $cluster, $workflowhash, $projectname, $workflowname, $workflownumber, $samplehash, $scheduler);
    $self->logDebug("no. stages", scalar(@$stages));
    #$self->logDebug("stages", $stages);

    #### GET FILEROOT
    my $fileroot = $self->util()->getFileroot($username);    
    $self->logDebug("fileroot", $fileroot);

    #### GET OUTPUT DIR
    my $outputdir =  "$fileroot/$projectname/$workflowname/";
    
    #### GET MONITOR
    my $monitor    =    $self->updateMonitor() if $scheduler eq "sge";

    #### SET FILE DIRS
    my ($scriptdir, $stdoutdir, $stderrdir) = $self->setFileDirs($fileroot, $projectname, $workflowname);
    $self->logDebug("scriptdir", $scriptdir);
    
    #### WORKFLOW PROCESS ID
    my $workflowpid = $self->workflowpid();

    $self->logDebug("DOING ALL STAGES stage->setStageJob()");
    foreach my $stage ( @$stages )  {
        #$self->logDebug("stage", $stage);
        my $installdir        =    $stage->installdir();
        $self->logDebug("installdir", $installdir);

        my $jobs    =    [];
        foreach my $samplehash ( @$sampledata ) {
            $stage->{samplehash}    =    $samplehash;
            
            push @$jobs, $stage->setStageJob();
        }
        $self->logDebug("no. jobs", scalar(@$jobs));

        #### SET LABEL
        my $stagename    =    $stage->name();
        $self->logDebug("stagename", $stagename);
        my $label    =    "$projectname.$workflowname.$stagename";

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
    my $success    =    $self->runStages($stages, $dryrun);
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

method runStages ($stages, $dryrun) {
    $self->logDebug("no. stages", scalar(@$stages));

    # #### SET EXCHANGE    
    # my $exchange = $self->conf()->getKey("core:EXCHANGE");
    # $self->logDebug("exchange", $exchange);
    
    #### SELF IS SIPHON WORKER
    my $worker    =    0;
    $worker        =    1 if defined $self->worker();
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
        
        my $username    =    $stage->username();
        my $projectname        =    $stage->projectname();
        my $workflowname    =    $stage->workflowname();
        
        my $mysqltime    =    $self->getMysqlTime();
        $self->logDebug("mysqltime", $mysqltime);
        $stage->started($mysqltime);
        
        #### CLEAR STDOUT/STDERR FILES
        my $stdoutfile    =    $stage->stdoutfile();
        `rm -fr $stdoutfile` if -f $stdoutfile;
        my $stderrfile    =    $stage->stderrfile();
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
            my $status    =    "completed";
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
        my $successor    =    $stage->getSuccessor();
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

    #### CREATE STAGE OBJECT FOR EACH STAGE TO BE RUN
    my $stageobjects = [];
    for ( my $counter = $start - 1; $counter < $stop - 1; $counter++ ) {
        my $stage = $$stages[$counter];
        $self->logDebug("stage", $stage);
        
        my $stagenumber    =    $stage->{appnumber};
        my $stagename        =    $stage->{appname};
        my $id                    =    $samplehash->{samplename};
        my $successor        =    $stage->{successor};
        $self->logDebug("successor", $successor) if defined $successor and $successor ne "";
        
        #### STOP IF NO STAGE PARAMETERS
        $self->logDebug("stageparameters not defined for stage $counter $stage->{appname}") and last if not defined $stage->{stageparameters};
        
        my $stage_number = $counter + 1;

        $stage->{username}        =      $username;
        $stage->{workflowpid}    =        $workflowpid;
        $stage->{table}                =        $self->table();
        $stage->{conf}                =      $self->conf();
        $stage->{fileroot}        =      $fileroot;
        $stage->{userhome}        =      $userhome;

        #### SET SGE ENVIRONMENT VARIABLES
        $stage->{envar} = $envar;
        
        #### MAX JOBS
        $stage->{maxjobs}        =    $self->maxjobs();

        #### SAMPLE HASH
        $stage->{samplehash}    =      $samplehash;
        $stage->{outputdir}        =      $outputdir;

        #### LOG
        $stage->{log}             =    $self->log();
        $stage->{printlog}         =    $self->printlog();
        $stage->{logfile}         =    $self->logfile();

    #### SET SCRIPT, STDOUT AND STDERR FILES
        $stage->{scriptfile}     =    "$scriptdir/$stagenumber-$stagename.sh";
        $stage->{stdoutfile}     =    "$stdoutdir/$stagenumber-$stagename.stdout";
        $stage->{stderrfile}     =     "$stderrdir/$stagenumber-$stagename.stderr";

        if ( defined $id ) {
            $stage->{scriptfile}     =    "$scriptdir/$stagenumber-$stagename-$id.sh";
            $stage->{stdoutfile}     =    "$stdoutdir/$stagenumber-$stagename-$id.stdout";
            $stage->{stderrfile}     =     "$stderrdir/$stagenumber-$stagename-$id.stderr";
        }

        my $stageobject = Engine::Local::Stage->new($stage);

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

method killStages ($stages) {
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



}    # class
