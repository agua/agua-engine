use MooseX::Declare;

class Test::Engine::Logic::Fork with (Test::Common, Agua::Common) extends Engine::Logic::Fork {

use Test::More;
use FindBin qw($Bin);

#use DBase::Factory;
use Engine::Workflow;

# Int
has 'log'		=>  ( isa => 'Int', is => 'rw', default => 4 );
has 'printlog'	=>  ( isa => 'Int', is => 'rw' );

# STRINGS
has 'dumpfile'	=> ( isa => 'Str|Undef', is => 'rw', default => '' );

# OBJECTS
has 'db'		=> ( isa => 'DBase::MySQL', is => 'rw', required => 0 );
has 'conf' 	=> (
	is =>	'rw',
	'isa' => 'Conf::Yaml',
	default	=>	sub { Conf::Yaml->new( backup	=>	1 );	}
);

method testSelectIndex {
	diag("selectIndex");

	my $tests = [
		{
			name		=>	"missing",
			inputfile	=>	"$Bin/inputs/selectindex/missing",
			expected	=>	1
		}
		,
		{
			name		=>	"present",
			inputfile	=>	"$Bin/inputs/selectindex/present",
			expected	=>	0
		}
		,
		{
			name		=>	"empty",
			inputfile	=>	"$Bin/inputs/selectindex/empty",
			expected	=>	undef
		}
	];

	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $inputfile	=	$test->{inputfile};
		my $expected	=	$test->{expected};
		$self->logDebug("name '$name' inputfile", $inputfile);			
	
		my $actual = $self->selectIndex($inputfile);
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);

		is_deeply($actual, $expected, $name);
	}
}

method testSelectBranch {
	diag("selectBranch");

	$self->logDebug("");

	my $tests = [
		{
			name		=>	"simple",
			tsvfiles	=>	[
				"$Bin/inputs/simple/stage.tsv",
				"$Bin/inputs/simple/stageparameter.tsv"
			],
			args	=>	{
				if			=>	2,
				else		=>	3,
				inputfiles	=>	"$Bin/inputs/simple/fork.in",
				outputfile	=>	"$Bin/outputs/simple/fork.out"
			},
			envars	=>	{
				USERNAME	=>	"testuser",
				PROJECT		=> 	"Project1",
				WORKFLOW	=>	"Workflow1"
			},
			expected	=>	{
				1	=>	{
					 successor 	=>	3,
					 status		=>	"completed"
				},
				2	=>	{
					ancestor	=>	undef,
					status		=>	"skip"
				},
				3	=>	{
					ancestor	=>	1,
					status		=>	"waiting"
				}
			}			
		}
	];

	#### SET KEY VALUE IN CONF FILE
	$self->conf()->setKey("agua", "EXCHANGE", "false");
	
	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $tsvfiles	=	$test->{tsvfiles};
		my $args		=	$test->{args};
		my $envars		=	$test->{envars};
		my $expected	=	$test->{expected};
		$self->logDebug("name '$name' expected", $expected);
			
		####### LOAD DATABASE
		$self->setUpTestDatabase();
		#$self->setDatabaseHandle();
	
		#### LOAD TSVFILE
		foreach my $tsvfile ( @$tsvfiles ) {
			my ($table) =	$tsvfile	=~ /([^\/]+)\.tsv$/;
			foreach my $arg ( keys %$args) {
				$self->logDebug("loading arg", $arg);
				$self->$arg($args->{$arg});
			}
			$self->logDebug("self->inputfiles", $self->inputfiles());

			foreach my $envar ( keys %$envars) {
				$self->logDebug("loading envar", $envar);
				$ENV{$envar}	=	$envars->{$envar};
			}
			$self->logDebug("ENV{'USERNAME'}", $ENV{'USERNAME'});

			#### CLEAN UP
			my $query = qq{DELETE FROM $table};
			$self->table()->db()->do($query);

			#### REPLACE TSV CONTENTS
			my $tempfile = $tsvfile;
			$tempfile =~ s/inputs/outputs/;
			$self->replaceContents($tsvfile, $tempfile);

			#### LOAD
			$self->logDebug("Loading tsvfile for table", $table);
			$self->loadTsvFile($table, $tempfile);
		}	

		$self->logDebug("BEFORE self->selectBranch()");
		$self->selectBranch();
		$self->logDebug("AFTER self->selectBranch()");
		
		my $query	=	"SELECT * FROM stage";
		my $stages	=	$self->table()->db()->queryhasharray($query);
		#$self->logDebug("stage#s", $stages);
		my @keys = keys %$expected;
		@keys = sort { $a <=> $b } @keys;
		foreach my $stagenumber ( @keys ) {
			$self->logDebug("stagenumber", $stagenumber);
			my $fields=	[ keys %{$expected->{$stagenumber}} ];
			$self->logDebug("fields", $fields);
			foreach my $field ( @$fields ) {
				$self->logDebug("field", $field);
				my $value	=	$expected->{$stagenumber}->{$field};
				$self->logDebug("value", $value);
	
				my $stage = $$stages[$stagenumber - 1];
				$self->logDebug("stage", $stage);
				my $expectedvalue = $stage->{$field};
				$self->logDebug("expectedvalue", $expectedvalue);
				is_deeply($expectedvalue, $value, "$name field '$field'");
			}
		}
		
	#	#$self->logDebug("result", $result);
	#	#is_deeply($result, $expected, "_updateProject $testname");
	#
	}
	$self->logDebug("completed");
}

method testBranchedWorkflow {
	diag("branchedWorkflow");

	$self->logDebug("");

	my $tests = [
		{
			name		=>	"simple",
			type		=>	"database",
			tsvfiles	=>	[
				"$Bin/inputs/simple/stage.tsv",
				"$Bin/inputs/simple/stageparameter.tsv"
			],
			data		=>	{
				username		=>	"testuser",
				project			=>	"Project1",
				workflow		=>	"Workflow1",
				workflownumber	=> 	1,
				start			=>	1,
				database		=>	"aguatest",
				scheduler		=>	"local"
			}
		}
		#,
		#{
		#	name		=>	"simple-cli",
		#	type		=>	"cli",
		#	appfiles	=>	[
		#		"$Bin/inputs/simple/stage.tsv",
		#		"$Bin/inputs/simple/stageparameter.tsv"
		#	]
		#}
	];

	#### SET KEY VALUE IN CONF FILE
	$self->conf()->setKey("agua", "EXCHANGE", "false");
	
	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $type		=	$test->{type};
		my $data		=	$test->{data};
		my $tsvfiles	=	$test->{tsvfiles};
		my $appfiles	=	$test->{appfiles};
		my $expected	=	$test->{expected};
		$self->logDebug("type", $type);
		$self->logDebug("data", $data);
	
		if ( $type eq "database" ) {
			
			####### LOAD DATABASE
			$self->setUpTestDatabase();
			#$self->setDatabaseHandle();
		
			my $tsvfiles	=	$test->{tsvfiles};
			#### LOAD TSVFILE
			foreach my $tsvfile ( @$tsvfiles ) {
				my ($table) =	$tsvfile	=~ /([^\/]+)\.tsv$/;
				
				#### CLEAN UP
				my $query = qq{DELETE FROM $table};
				$self->table()->db()->do($query);

				#### REPLACE TSV CONTENTS
				my $tempfile = $tsvfile;
				$tempfile =~ s/inputs/outputs/;
				$self->replaceContents($tsvfile, $tempfile);

				#### LOAD
				$self->logDebug("Loading tsvfile for table", $table);
				$self->loadTsvFile($table, $tempfile);
			}	
		}
		$self->logDebug("AFTER type IF");
		
		#### TEST
		my $workflow	=	Engine::Workflow->new(
			db		=>	$self->table()->db(),
			conf	=>	$self->conf(),
			log		=>	$self->log(),
			printlog=>	$self->printlog()
		);
		#$self->logDebug("workflow", $workflow);
		$workflow->executeWorkflow($data);
		
		#$self->logDebug("result", $result);
		#is_deeply($result, $expected, "_updateProject $testname");
	
	}
	$self->logDebug("completed");
}

method replaceContents ($sourcefile, $targetfile) {
	my $contents 	=	$self->getFileContents($sourcefile);
	#$self->logDebug("contents", $contents);
	
	$contents	=~	s/<PWD>/$Bin/g;
	#$self->logDebug("AFTER REGEX contents", $contents);
	$self->printToFile($targetfile, $contents);
}



}   #### Test::Common::Cluster