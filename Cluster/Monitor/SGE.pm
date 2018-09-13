use MooseX::Declare;

=head2

	PACKAGE		Engine::Cluster::Monitor::SGE
	
  PURPOSE
  
    MONITOR JOBS RUN ON AN SGE (PORTABLE BATCH SCHEDULER) SYSTEM

	HISTORY
	
		0.03	Streamlined by removing SQLite-specific methods
		0.02	Added monitor_jobs failback on 'ERROR' qstat output
		0.01	Basic version

	NOTES
	
		1. GET REMAINING JOBS USING QSTAT WITH WAIT & RETRY IF QSTAT FAILS
		
		QSTAT WITH JOB ID	
			remainingJobs 
				IF BATCH JOB RETURN remainingBatchJobs
				ELSE
				-->	jobLines: RETURN QSTAT CALL RESULTS AS ARRAY OF LINES
					-->	repeatTries

			remainingBatchJobs: RETURN ARRAY OF JOBS STILL QUEUED/RUNNING
				-->	getTasks: RETURN ARRAY OF TASK IDS FROM BATCH JOB ID
			
		jobStatus
		-->	checkToStatus


		PLAIN VANILLA QSTAT WITH SEARCH FOR JOB ID AMONG LINES
		
		statusHash
			-->jobLines: RETURN QSTAT CALL RESULTS AS ARRAY OF LINES
				-->	lineStatus: PARSE STATUS FROM LINE (r, t, qw, Eqw) INTO
						STATES 'running', 'starting', 'queued' AND 'error'


		jobStatus
		-->	jobLines: RETURN QSTAT CALL RESULTS AS ARRAY OF LINES
		--> jobLineStatus: RETURN THE STATUS OF A PARTICULAR JOB ID
			--> lineStatus: RETURN 'running', 'starting', ETC. OF JOB ID

=cut 

use strict;
use warnings;
use Carp;

class Engine::Cluster::Monitor::SGE with (Util::Logger, 
	Engine::Common::SGE, 
	Engine::Common::Ssh, 
	Util::Timer) {

#### EXTERNAL MODULES
use FindBin qw($Bin);
use lib "$Bin/../..";
use POSIX;
use File::Path;

#### INTERNAL MODULES
use DBase::Factory;
use Engine::Cloud::Instance;
use Util::Main;

#### Boolean
has 'loaded'		=> ( isa => 'Bool', is => 'rw', default => 0 );  
has 'log'			=>  ( isa => 'Int', is => 'rw', default => 1 );  
has 'printlog'			=>  ( isa => 'Int', is => 'rw', default => 2 );

#### Int
has 'pid'		=> ( isa => 'Int|Undef', is => 'rw' );
has 'tries'		=> ( isa => 'Int|Undef', is => 'rw', default => 20 ); 	#### BEFORE QUIT
has 'sleep'		=> ( isa => 'Int|Undef', is => 'rw', default => 5 );

#### String
has 'outputdir'	=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'whoami'	=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'configfile'=> ( isa => 'Str|Undef', is => 'rw', default => '' );
has 'queue'		=> ( isa => 'Str|Undef', is => 'rw' );
has 'cluster'	=> ( isa => 'Str|Undef', is => 'rw' );
has 'username'  => ( isa => 'Str|Undef', is => 'rw' );
has 'workflow'  => ( isa => 'Str|Undef', is => 'rw' );
has 'project'   => ( isa => 'Str|Undef', is => 'rw' );
has 'qsub'		=> ( isa => 'Str|Undef', is => 'rw', required => 0 );
has 'qstat'		=> ( isa => 'Str|Undef', is => 'rw', required => 0 );
has 'errorregex'=> ( isa => 'Str|Undef', is => 'rw', default => "(error|unable to contact)");
has 'jobidregex'=> ( isa => 'Str|Undef', is => 'rw', default => qq{^(Your job|Your job-array) (\\S+)});

# Hash/Array
has 'envarsub'	=> ( isa => 'Maybe', is => 'rw', lazy => 1, builder => "setEnvarsub" );
has 'customvars'=>	( isa => 'HashRef', is => 'rw', default => sub {
	return {
		cluster 		=> 	"CLUSTER",
		qmasterport 	=> 	"SGE_MASTER_PORT",
		execdport 		=> 	"SGE_EXECD_PORT",
		sgecell 		=> 	"SGE_CELL",
		sgeroot 		=> 	"SGE_ROOT",
		queue 			=> 	"QUEUE"
	};
});

#### Object
has 'ssh'			=> ( isa => 'Util::Ssh', is => 'rw', required	=>	0	);
has 'conf'		=> ( isa => 'Conf::Yaml', is => 'rw', required => 0 );
has 'db'	=> ( isa => 'Any', is => 'rw', required => 0 );
# has 'head' 	=> (
# 	is =>	'rw',
# 	'isa' => 'Engine::Cloud::Instance',
# 	default	=>	sub { Engine::Cloud::Instance->new();	}
# );
# has 'master' 	=> (
# 	is =>	'rw',
# 	'isa' => 'Engine::Cloud::Instance',
# 	default	=>	sub { Engine::Cloud::Instance->new();	}
# );

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
		table		=>	$self->table(),
		conf		=>	$self->conf(),
		customvars	=>	$customvars,
		envarsub	=>	$envarsub,
		parent		=>	$self
	});
	
	$self->envar($envar);
}

# has 'table'		=>	(
# 	is 			=>	'rw',
# 	isa 		=>	'Table::Main',
# 	lazy		=>	1,
# 	builder	=>	"setTable"
# );

# method setTable {
# 	$self->logCaller("");
# 	print "Engine::Cluster::Monitor::SGE   DOING setTable\n";
# 	exit;

# 	my $table = Table::Main->new({
# 		conf			=>	$self->conf(),
# 		log				=>	$self->log(),
# 		printlog	=>	$self->printlog()
# 	});

# 	$self->table($table);	
# }


method BUILD ($args) {
	$self->log(5);
	$self->logCaller("");
	$self->logDebug("DOING self->loadArgs()");
	$self->loadArgs($args);
	
	$self->logDebug("DOING self->initialise()");
	$self->initialise();
}

method loadArgs ($args) {
	$self->logDebug("");

	#### IF HASH IS DEFINED, ADD VALUES TO SLOTS
	if ( defined $args ) {
		foreach my $key ( keys %{$args} ) {
			$args->{$key} = $self->unTaint($args->{$key});
			my $ref = ref $args->{$key};
			my $isobject = 1;
			$isobject = 0 if not $ref;
			$isobject = 0 if $ref eq "HASH";
			$isobject = 0 if $ref eq "ARRAY";
			#$self->logDebug("ADDING key $key", $args->{$key}) if not $isobject;
			#$self->logDebug("ADDING key $key: $args->{$key}") if $isobject;
			$self->$key($args->{$key}) if $self->can($key);
		}
	}

    $self->logDebug("Completed");
}

method unTaint ($input) {
  return if not defined $input;

  $input =~ s/;.*$//g;
  $input =~ s/`.*$//g;

  return $input;
}

method load ($args) {
	$self->logDebug("");
	#$self->logDebug("args", $args);

	#$self->logDebug("DOING self->clear()");
	#$self->clear();
	
	$self->logDebug("DOING self->loadArgs()");
	$self->loadArgs($args);
	
	$self->logDebug("DOING self->initialise()");
	$self->initialise();

	$self->loaded(1);

	return $self;
}

method clear {
	$self->logCaller("");
	my $meta = Engine::Cluster::Monitor::SGE->meta();

	#### GET ATTRIBUTES
	my $attributes;
	@$attributes = $meta->get_attribute_list();
	$self->logDebug("attributes", $attributes);

	#### RESET TO DEFAULT OR CLEAR ALL ATTRIBUTES
	foreach my $attribute ( @$attributes ) {
        next if $attribute eq "log";
        next if $attribute eq "printlog";
        next if $attribute eq "db";
        
		my $attr = $meta->get_attribute($attribute);
		my $required = $attr->is_required;
		$required = "undef" if not defined $required;
		my $default 	= $attr->default;
		my $isa  		= $attr->{isa};
		$isa =~ s/\|.+$//;		
		my $ref = ref $default;
		my $value 		= $attr->get_value($self);
		#$self->logDebug("$attribute: $isa value", $value);
		next if not defined $value;

		if ( not defined $default ) {
			$attr->clear_value($self);
		}
		else {
			#$self->logDebug("SETTING VALUE TO DEFAULT", $default);
			if ( $ref ne "CODE" ) {
				$attr->set_value($self, $default);
			}
			else {
				$attr->set_value($self, &$default);
			}
		}
		$self->logNote("CLEARED $attribute ($isa)", $attr->get_value($self));
	}

	$self->loaded(0);
}

method initialise () {
	$self->logDebug("");
	my $cluster = $self->cluster();
	my $username = $self->username();
	$self->logDebug("cluster", $cluster);
	$self->logDebug("username", $username);
	return if not defined $username or not defined $cluster or not defined $self->conf();
	
	#### DETERMINE WHETHER TO USE ADMIN KEY FILES
	my $adminkey = $self->getAdminKey($username);
	$self->logDebug("adminkey", $adminkey);
	return if not defined $adminkey;
	my $adminuser	=	$self->conf()->getKey("core:ADMINUSER");	
	$self->logDebug("adminuser", $adminuser);

	my $logfile;
	$logfile = $self->getMonitorLogfile($username, $cluster) if not $adminkey;
	$logfile = $self->getMonitorLogfile($adminuser, $cluster) if $adminkey;
	$self->logDebug("logfile", $logfile);

	# #### SET CLUSTER INSTANCES LOG
	# $self->head()->logfile($logfile);
	# $self->head()->log($self->log());
	# $self->head()->printlog($self->printlog());
	# $self->master()->logfile($logfile);
	# $self->master()->log($self->log());
	# $self->master()->printlog($self->printlog());
	
	# #### SET HEADNODE OPS LOG
	# $self->head()->ops()->logfile($logfile);	
	# $self->head()->ops()->log($self->log());
	# $self->head()->ops()->printlog($self->printlog());

	# #### SET HEADNODE OPS CONF
	# my $conf 	= 	$self->conf();
	# $self->head()->ops()->conf($conf);	

	# #### SET MASTER OPS LOG
	# $self->master()->ops()->logfile($logfile);	
	# $self->master()->ops()->log($self->log());
	# $self->master()->ops()->printlog($self->printlog());

	# #### SET MASTER OPS CONF
	# $self->master()->ops()->conf($conf);	
	
	#### SET qstat EXECUTABLE LOCATION
	$self->logDebug("Doing self->setQstat()");
	$self->setQstat();
}

method getMonitorLogfile ($username, $cluster) {
	#### GET USERDIR AND AGUADIR
	my $userdir 	= 	$self->conf()->getKey("core:USERDIR");
	my $aguadir 	= 	$self->conf()->getKey("core:AGUADIR");
	my $outputdir 	=	"$userdir/$username/$aguadir/.cluster";

	return "$outputdir/$cluster-jobscheduler.log";
}

method setQstat {
	my $conf = $self->conf();

	my $qstat = $conf->getKey("scheduler:QSTAT");
	$qstat = $conf->getKey("scheduler:QSTAT") if not defined $qstat;
	$self->logError("sgeroot not defined") and exit if not defined $qstat;

	$self->qstat($qstat);	
}

method qacct ($username, $cluster, $jobid) {
	#### MAKE qacct CALL TO MASTER
	$self->logDebug("cluster", $cluster);

	my $envars = $self->envar()->toString();
	# my $sgeroot = $self->conf()->getKey("scheduler:SGEROOT");
	my $sgebin = $self->conf()->getKey("scheduler:SGEBIN");

	#### STARCLUSTER
	# my $keypairfile = $self->setKeypairFile($username);
	# my $masterip = $self->getActQmaster($cluster);
	# my $masterip = $self->conf()->getKey("scheduler:MASTERIP");
	# $self->logDebug("masterip", $masterip);
	# $self->_setSsh("root", $masterip, $keypairfile);
	# my $sgebin = $self->master()->ops()->getSgeBinRoot();	
	
	my $scheduler	=	$self->conf()->getKey("core:SCHEDULER");
	$self->logDebug("scheduler", $scheduler);
	my $command 	=	"$envars $sgebin/qacct -j $jobid";
	my $qacct 	= 	`$command`;
	$qacct	=~	s/\s+$//;

	#### STARCLUSTER
	# my $qacct;
	# if ( $scheduler eq "starcluster" ) {
	# 	($qacct) = $self->ssh()->execute($command);
	# }
	# else {
		# $qacct 	= 	`$command`;
		# $qacct	=~	s/\s+$//;
	# }
	
	return $qacct;
}

method setEnv {
#### SET SGE ENVIRONMENT VARIABLES
	$self->logDebug("Engine::Cluster::Monitor::SGE::setEnv()");

	my $conf = $self->conf();
	my $sgeroot = $conf->getKey("agua:SGEROOT");
	$sgeroot = $conf->getKey("scheduler:SGEROOT") if not defined $sgeroot;
	my $qmasterport = $conf->getKey("scheduler:SGEQMASTERPORT");
	$qmasterport = $conf->getKey("scheduler:SGEQMASTERPORT") if not defined $qmasterport;
	my $execdport = $conf->getKey("scheduler:SGEEXECDPORT");
	$execdport = $conf->getKey("scheduler:SGEEXECDPORT") if not defined $execdport;

	#### CHECK INPUTS
	$self->logError("sgeroot not defined") and exit if not defined $sgeroot;
	$self->logError("qmasterport not defined") and exit if not defined $qmasterport;
	$self->logError("execdport not defined") and exit if not defined $execdport;

	$ENV{'SGE_ROOT'} = $sgeroot;
	$ENV{'SGE_QMASTER_PORT'} = $qmasterport;
	$ENV{'SGE_EXECD_PORT'} = $execdport;
}

method submitJob ($job) {
=head2

	SUBROUTINE		submitJob
	
	PURPOSE
	
		SUBMIT A JOB TO THE CLUSTER AND RETURN THE JOB ID
		
		IF JOB ID IS UNDEFINED, REPORT AND QUIT
		
=cut
	$self->logDebug("Engine::Cluster::Monitor::SGE::submitJob(command)");
	$self->logDebug("job", $job);
	
	my $queue 		= 	$job->{queue};
	my $batch 		= 	$job->{batch};
	$batch = '' if not defined $batch;
	my $scriptfile 	= 	$job->{scriptfile};
	my $qsub 		=	$job->{qsub};
	#$self->logDebug("qsub", $qsub);
	
	my $walltime 	=	$job->{walltime};

	#### CHECK INPUTS
	$self->logDebug("queue not defined. Returning") and return if not defined $queue;
	$self->logDebug("qsub not defined. Returning") and return if not defined $qsub;
	$self->logDebug("Scriptfile not defined. Returning null") and return if not defined $job->{scriptfile};

	#### SET SGE_CELL TO DETERMINE WHICH CLUSTER TO RUN JOB ON
	my $cluster = $job->{cluster};
	$ENV{'SGE_CELL'} = $cluster if defined $cluster;

	#### SET COMMAND
	my $command;
	
	#### SET ENVIRONMENT VARIABLES
	$command .= $job->{envars}->{tostring} if defined $job->{envars};

	#### SET MAXIMUM WALLTIME FOR THIS JOB
	#### -l h_rt=hr:min:sec
	$walltime = " -l h_rt=$walltime " if defined $walltime;
	$walltime = " -l h_rt=24:00:00 " if not defined $walltime;
	
	#### SET RERUNNABLE BY DEFAULT
	my $rerunnable = "-r y";
	
	#### SET QSUB LINE
	$command .= "$qsub $batch -q $queue -V $walltime $rerunnable $scriptfile 2>&1";
	$self->logDebug("command", $command);

	#### SUBMIT JOB
	my $output = `$command` || "";
	$self->logDebug("AFTER SUBMIT");
	#wait;
	$output =~ s/\n/ /g;
	$output =~ s/"/'/g;
	$self->logDebug("output", $output);
	if ( not $output or $output =~ /error: commlib error/ ) {
		return undef, "Can't connect to qmaster. Command output: $output";
	}
	
	#### OUTPUT FORMAT:
	#### Your job-array 214.1-10:1 ("bowtie-chr22") has been submitted
	my $jobid_regex = $self->jobidregex();
	use re 'eval';	# EVALUATE AS REGEX
	my ($job_type, $job_id) = $output =~ /$jobid_regex/;
	no re 'eval';	# STOP EVALUATE AS REGEX
	$self->logError("job_id not defined") and exit if not defined $job_id;
	$self->logDebug("job_id after submit", $job_id)  if defined $job_id;
	$self->logDebug("job_id not defined after submit")  if not defined $job_id;	;

	$self->logDebug("returning job_id", $job_id);
	return $job_id;	
}


#### ORDINARY JOB STATUS USING QSTAT WITH JOB ID
method remainingJobs ($job_ids) {
=head2

    SUBROUTINE      remainingJobs
    
    PURPOSE
    
        RETURN THE LIST OF JOB IDS STILL QUEUED OR RUNNING AMONG
		
		THE LIST SUPPLIED
		
	NOTES
	
		FINISHED JOBS DISAPPEAR IMMEDIATELY FROM THE qstat
		
		OUTPUT BUT THERE MAY BE A LAG BETWEEN SUBMISSION AND
		
		THE JOB APPEARING IN THE qstat OUTPUT SO TRY SEVERAL
		
		TIMES IF A JOB DOESN'T APPEAR TO BE IN THE QUEUE.
		
=cut
	$self->logDebug("job_ids: @$job_ids");
	return $self->remainingBatchJobs($job_ids) if $$job_ids[0] =~ /^\d+\.\d+/;

	##### GET LIST OF JOBS IN QSTAT
	my $matched = [];
	my $tries = 3;
	my $lines = $self->jobLines();
	while ( @$job_ids and $tries )
	{
		for ( my $i = 0; $i < @$job_ids; $i++ )
		{
			my $job_id = $$job_ids[$i];
			foreach my $line ( @$lines )
			{
				if ( $line =~ /^\s*$job_id\s+/ )
				{
					push @$matched, splice @$job_ids, $i, 1;
					$i--;
					last;
				}
			}
		}
		
		last if not @$job_ids;
		
		$tries--;
		sleep(5);
		$lines = $self->jobLines();
	}
	
    return $matched;
}


#### BATCH JOB STATUS USING QSTAT WITH JOB ID
method remainingBatchJobs ($job_ids){	
=head2

    SUBROUTINE      remainingBatchJobs
    
    PURPOSE
    
        RETURN THE LIST OF PIDS CURRENTLY IN THE QUEUE
		
	NOTES
	
		PARSE OUT JOBARRAY IDS FROM qstat OUTPUT TO DETERMINE
		
		IF JOBS ARE STILL QUEUED OR RUNNING. RETURN THE LIST 
		
		OF JOBS STILL QUEUED OR RUNNING
		
=cut
	$self->logDebug("Engine::Cluster::Monitor::SGE::remainingBatchJobs(job_ids)");
	$self->logDebug("job_ids", $job_ids);
	
	my $matched = [];
	my $tries = 3;
	my $lines = $self->jobLines();
	$self->logDebug("lines", $lines);
	
	
	while ( @$job_ids and $tries )
	{
		$self->logDebug("tries left: ", $tries - 1, "");

		for ( my $i = 0; $i < @$job_ids; $i++ )
		{
			$self->logDebug("job_ids[$i]", $$job_ids[$i]);
			my ($job_id, $task_info) = $$job_ids[$i] =~ /^(\d+)\.(.+)$/;
			$self->logDebug("[][][][][][][][][][][][][][][][][][][][][][][][][][][][][]");
			$self->logDebug("job_id", $job_id);
			$self->logDebug("task_info", $task_info);
			
			my $tasks = $self->getTasks($task_info);
			$self->logDebug("tasks: @$tasks");

			#### JOB ID FORMAT:
			#### Your job-array 214.1-10:1 ("bowtie-chr22") has been submitted
			my $hit = 0;
			foreach my $line ( @$lines )
			{
				next if not $line =~ /^\s*(\d+)\s+.+?(\S+)\s*$/;
				my $current_job_id = $1;
				my $current_task_info = $2;
				$self->logDebug("*********************************");
				$self->logDebug("current_task_info", $current_task_info);
				$self->logDebug("current_job_id", $current_job_id);
				$self->logDebug("current_task_info", $current_task_info);
				
				if ( $job_id == $current_job_id )
				{
					$self->logDebug("job_id ($job_id) and current_job_id ($current_job_id) match. Checking tasks");
					
					my $current_tasks = $self->getTasks($current_task_info);
					$self->logDebug("current_tasks: @$current_tasks");

					foreach my $task ( @$tasks )
					{
						$self->logDebug("task", $task);
						foreach my $current_task (@$current_tasks)
						{
							$self->logDebug("current_task", $current_task);
							if ( $task == $current_task )
							{
								$self->logDebug("task $task matched currrent task", $current_task);
								$hit = 1;
								last;
							}
						}
						
						last if $hit;
						
					}	#### COMPARE TASKS
					$self->logDebug("final value of hit", $hit);

					if ( $hit )
					{
						$self->logDebug("splicing job_ids at $i");
						
						push @$matched, splice @$job_ids, $i, 1;
						$i--;
						last;
					}

				}	#### COMPARE job_id AND current_job_id

				last if $hit;

			}	#### lines			
			
			$self->logDebug("matched: @$matched");
		}
		
		last if not @$job_ids;

		$tries--;
		sleep(5);
	}
	
	$self->logDebug("Returning matched: @$matched\n\n");
	
    return $matched;
}

method getTasks ($task_info) {
	$self->logDebug("task_info", $task_info);
	
	my $tasks;
	push @$tasks, $task_info and return $tasks if $task_info =~ /^\d+$/;

	push @$tasks, split ",", $task_info and return $tasks if $task_info =~ /^\d+,\d+$/;

	#### HANDLE BATCH JOB
	my ($task_ids, $step) = $task_info =~ /^(\S+)\:(\d+)$/;
	if ( $task_ids =~ /^(\d+)-(\d+)$/ )
	{
		my $start = $1;
		my $stop = $2;
		
		for ( my $i = $start; $i <= $stop; $i+=$step )
		{
			push @$tasks, $i;
		}
	}
	
	$self->logDebug("tasks: @$tasks");

	return $tasks;
}



#### GET STATUS FOR ALL QUEUED/RUNNING JOBS FROM PLAIN VANILLA QSTAT
method statusHash {
#### HASH JOBID AGAINST STATUS
	$self->logNote("Engine::Cluster::Monitor::SGE::statusHash()");
	my $joblines = $self->jobLines();
	return {} if not defined $joblines;
	$self->logNote("joblines: @$joblines");

	my $statusHash;
	foreach my $line ( @$joblines )
	{
		$self->logNote("line", $line);

		if ( $line =~ /^\s*(\d+)/ )
		{
			$self->logNote("pid", $1);
			$self->logNote("lineStatus: " . $self->lineStatus($line));
			$statusHash->{$1} = $self->lineStatus($line);
		}
	}
	
	$self->logNote("statusHash", $statusHash);
	
	return $statusHash;
}

method lineStatus ($line) {	
=head2

	SUBROUTINE		lineStatus
	
	PURPOSE
	
		RETURN THE STATUS ENTRY FOR A JOB LINE

	NOTES

		SGE JOB LIFECYCLE:
		
		"qw"	queued and waiting
		"t"		transferring to an available node
		"r" 	running
		"Eqw"	error state
		
		When a job no longer appears on the qstat output, it has finished or been deleted.
	
		QSTAT FORMAT:
		job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID 
		-----------------------------------------------------------------------------------------------------------------
			135 0.50000 tophatBatc www-data     Eqw   04/13/2011 00:39:46                                    3 1
			136 0.50000 tophatBatc www-data     Eqw   04/13/2011 01:14:03                                    3 1
			170 0.00000 tophatBatc root         qw    04/13/2011 17:00:25                                    3 1      

=cut
	$self->logNote("line", $line);
	my ($status) = $line =~ /^\s*\S+\s+\S+\s+\S+\s+\S+\s+(\S+)/;
	$self->logNote("status", $status);
	
	return "running" if $status eq "r";
	return "starting" if $status eq "t";
	return "queued" if $status eq "qw";
	return "error" if $status eq "Eqw";
}


#### RUN QSTAT AND PARSE JOB IDS 
method jobLines {
=head2

	SUBROUTINE		jobLines
	
	PURPOSE
		
		RETURN THE LINES FROM A QSTAT CALL:

			EXEC QSTAT COMMAND AND COLLECT RESULT LINES AS THEY <STREAM> OUT
	
=cut
	$self->logNote("Engine::Cluster::Monitor::SGE::jobLines()");

	#### TRY REPEATEDLY TO GET A CLEAN QSTAT REPORT
	my $sleep		=	$self->sleep();	
	my $tries		=	$self->tries();	

	#### GET ERROR MESSAGES ALSO
	my $sgebin = $self->sgeBinCommand("head");
	my $command = "$sgebin/qstat 2>&1 |";
	$self->logNote("command", $command);
	
	#### REPEATEDLY TRY SYSTEM CALL UNTIL A NON-ERROR RESPONSE IS RECEIVED
	my $error_regex = $self->errorregex();

	$self->logNote("Doing repeatTries(command, sleep, tries)");
	my $result = $self->repeatTries($command, $sleep, $tries);
	my @lines = split "\n", $result;
	my $jobs_list = [];
	foreach my $line ( @lines )
	{
		use re 'eval';# EVALUATE AS REGEX
		my $jobid_regex = $self->jobidregex();
		push @$jobs_list, $line if $line =~ /$jobid_regex/;
		no re 'eval';# EVALUATE AS REGEX
	}
	
	return \@lines;
}

#### REPEAT COMMAND UNTIL NON-ERROR RETURNED
method repeatTries ($command, $sleep, $tries) {
####	REPEATEDLY TRY SYSTEM CALL UNTIL A NON-ERROR RESPONSE IS RECEIVED
	$self->logNote("Engine::Cluster::Monitor::SGE::repeatTries(command)");
	$self->logNote("command", $command);
	$self->logNote("sleep", $sleep);
	$self->logNote("tries", $tries);
	
	my $result = '';	
	my $error_message = 1;
	while ( $error_message and $tries )
	{
		open(COMMAND, $command) or die "Can't exec command: $command\n";
		while(<COMMAND>) {
			$result .= $_;
		}
		close (COMMAND);
		$self->logNote("qstat result", $result);

		use re 'eval';	# EVALUATE AS REGEX
		my $error_regex = $self->errorregex();
		$error_message = $result =~ /$error_regex/;
		$self->logNote("error_message", $error_message)  if not $result;
		no re 'eval';# STOP EVALUATING AS REGEX

		#### DECREMENT TRIES AND SLEEP
		$tries--;
		$self->logNote("$tries tries left.")  if not $result;
		$self->logNote("qstat sleeping $sleep seconds") if $error_message;

		$self->logNote("current datetime: ");
		$self->logNote(`date`);

		sleep($sleep) if $error_message;
	}
	$self->logNote("Returning result", $result);
	
	return $result;
}




method jobStatus ($job_id) {
#### RETURN THE STATUS OF A PARTICULAR JOB IDENTIFIED BY JOB ID
#### NB: CALLED IN Sampler.pm BY fastaInfos AND printFiles SUBROUTINES
	$self->logNote("job_id", $job_id);
	
	my $qstat_output = $self->jobLines();
	$self->logNote("qstat_output", $qstat_output);

	my $status = $self->jobLineStatus($job_id, $qstat_output);
	$self->logNote("status", $status);

	#my $jobcheck = $self->checkJob($job_id);	
	#
	#my $status = $self->checkToStatus($jobcheck);
  return $status;
}

method checkJob ($job_id) {
#### GET RESULTS FROM qstat CALL
	$self->logNote("Engine::Cluster::Monitor::SGE::checkJob(job_id)");
	die "Engine::Cluster::Monitor::SGE::checkJob    job_id not defined. Exiting.\n" if not defined $job_id;
	$self->logNote("job_id", $job_id);

	#qstat -a, -i, -r, -u, -n, -s, -G or -M 
	my $qstat = $self->qstat();
	die "Engine::Cluster::Monitor::SGE::checkJob    qstat not defined. Exiting.\n" if not defined $qstat;

	#### SET COMMAND
	my $envars = $self->envars();
	die "Engine::Cluster::Monitor::SGE::checkJob    envars not defined. Exiting.\n" if not defined $envars;
	
	my $command = "$envars->{tostring} $qstat -j $job_id 2>&1 |";
	$self->logNote("command", $command);

	#### SLEEP BETWEEN TRIES
	my $sleep		=	$self->sleep();	
	my $tries		=	$self->tries();	
	$self->logNote("sleep", $sleep);

	#### REPEATEDLY TRY SYSTEM CALL UNTIL A NON-ERROR RESPONSE IS RECEIVED
	#### OR UNTIL A PRESET NUMBER OF TRIES HAVE ELAPSED
	my $result = $self->repeatTries($command, $sleep, $tries);
	$self->logNote(":checkJob    result", $result);
	
	return $result;	
}

method checkToStatus ($jobcheck) {
####	PARSE qstat OUTPUT TO GET JOB STATUS
	$self->logNote("Engine::Cluster::Monitor::SGE::checkToStatus(jobcheck)");

	my $statusHash;
	($statusHash->{status}) 	= $jobcheck =~ /State:\s*(\S+)/ms;
	($statusHash->{submittime}) = $jobcheck =~ /submission_time:\s*([^\n]+)/ms;
	($statusHash->{starttime}) 	= $jobcheck =~ /StartTime:\s*([^\n]+)/ms;
	($statusHash->{nodes}) 		= $jobcheck =~ /Allocated Nodes:\s*\n\s*(\S+?)/ms;
	($statusHash->{nodecount}) 	= $jobcheck =~ /NodeCount:\s*(\d+)/ms;

	$self->logNote("statusHash", $statusHash);

	($statusHash->{status}) 	||= '';
	($statusHash->{submittime}) ||= '';
	($statusHash->{starttime})  ||= '';
	($statusHash->{nodes})  	||= '';
	($statusHash->{nodecount})  ||= '';

	$self->logNote("statusHash", $statusHash);

	return $statusHash;	
}


method jobLineStatus ($job_id, $job_lines) {
=head2

    SUBROUTINE      jobLineStatus
    
    PURPOSE
    
        RETURN THE STATUS OF A PARTICULAR JOB IDENTIFIED BY JOB ID
        
	NOTES
	
		CALLED IN Sampler.pm BY fastaInfos AND printFiles SUBROUTINES
	
=cut
	$self->logNote("Engine::Cluster::Monitor::SGE::jobLineStatus(pids, qstat, sleep)");
	$self->logNote("job_id", $job_id)  if defined $job_id;
	$self->logNote("job_lines: @$job_lines");

	#### GET LIST OF JOBS IN QSTAT
	my $lines = $self->jobLines();

	my $status = "completed";
    foreach my $line ( @$lines )
    {
		$self->logNote("line", $line);
		
		my $match = $line =~ /^\s*$job_id\D+/;
		$self->logNote("match", $match);

		if ( $match )
		{
			$status = $self->lineStatus($line);
			last;
        }
    }

	$self->logNote("Returning status", $status);

    return $status;
}


method jobIds {
	$self->logNote("Engine::Cluster::Monitor::SGE::jobIds()");	;

	#### GET LIST OF JOBS IN QSTAT
	my $lines = $self->jobLines();

	#### PARSE OUT IDS FROM LIST
	my $job_ids = [];
	foreach my $line ( @$lines )
	{
		use re 'eval';# EVALUATE AS REGEX
		my $jobid_regex = $self->jobidregex();
		$line =~ /$jobid_regex/;
		push @$job_ids, $1 if defined $1 and $1;
		no re 'eval';# STOP EVALUATING AS REGEX
	}

	return $job_ids;
}

#### ENVAR

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
	return if not defined $values->{project};
	return if not defined $values->{workflow};
	
	return $values->{username} . "." . $values->{project} . "." . $values->{workflow};
}


}

