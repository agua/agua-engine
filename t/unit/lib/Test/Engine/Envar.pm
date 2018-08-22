use Moose::Util::TypeConstraints;
use MooseX::Declare;
use Method::Signatures::Modifiers;

class Test::Envar extends Envar with Util::Logger {

use Test::More;
use FindBin qw($Bin);

#####/////}}}}}

method BUILD ($args) {
	if ( defined $args ) {
		foreach my $arg ( $args ) {
			$self->$arg($args->{$arg}) if $self->can($arg);
		}
	}
	
	$self->initialise();
}


method testAddCustomVars {
	diag("addCustomVars");
	
	my $tests = [
	
		{
			name		=>	"customVars",
			customvars 	=>	{
				qmasterport 	=> 	"SGE_MASTER_PORT",
				execdport 		=> 	"SGE_EXECD_PORT",
				sgecell 		=> 	"SGE_CELL",
				sgeroot 		=> 	"SGE_ROOT",
				queue 			=> 	"QUEUE"
			},
			expected	=>	{
				username		=>	"USERNAME",
				project			=>	"PROJECT",
				workflow		=>	"WORKFLOW",
				workflownumber	=>	"WORKFLOWNUMBER",
				stage			=>	"STAGE",
				stagenumber		=>	"STAGENUMBER",
				sample			=>	"SAMPLE",
				sessionid		=>	"SESSION_ID",
				qmasterport 	=> 	"SGE_MASTER_PORT",
				execdport 		=> 	"SGE_EXECD_PORT",
				sgecell 		=> 	"SGE_CELL",
				sgeroot 		=> 	"SGE_ROOT",
				queue 			=> 	"QUEUE"
			}
		}
	];
	
	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $customvars	=	$test->{customvars};
		my $expected	=	$test->{expected};
		$self->logDebug("name", $name);

		#### MANUALLY SET VALUES		
		my $envars = $self->_addCustomVars($customvars);
		$self->logDebug("envars", $envars);
		$self->logDebug("expected", $expected);
		
		is_deeply($envars, $expected, $name);		
	}
}

method testGetVar {
	diag("getVar");
	
	my $tests = [
		{
			name		=>	"simple",
			variable	=>	"workflow",
			values		=>	{
				project 	=>	"Project1",
				workflow 	=>	"Workflow1"
			},
			expected	=>	"Workflow1"
		},
		{
			name		=>	"customVars",
			variable	=>	"sgecell",
			values		=>	{
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1"
			},
			customvars 	=>	{
				qmasterport 	=> 	"SGE_MASTER_PORT",
				execdport 		=> 	"SGE_EXECD_PORT",
				sgecell 		=> 	"SGE_CELL",
				sgeroot 		=> 	"SGE_ROOT",
				queue 			=> 	"QUEUE"
			},
			expected	=>	"testuser.Project1.Workflow1"
		},
		{
			name		=>	"runEnvarSub",
			variable	=>	"queue",
			values		=>	{
				queue		=>	"testuser.Project1.Workflow1",
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1"
			},
			customvars 	=>	{
				qmasterport 	=> 	"SGE_MASTER_PORT",
				execdport 		=> 	"SGE_EXECD_PORT",
				sgecell 		=> 	"SGE_CELL",
				sgeroot 		=> 	"SGE_ROOT",
				queue 			=> 	"QUEUE"
			},
			envarsub 	=>	*testEnvarSub,
			expected	=>	"testuser.Project1.Workflow1"
		}
	];
	
	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $variable	=	$test->{variable};
		my $values		=	$test->{values};
		my $customvars	=	$test->{customvars};
		my $envarsub	=	$test->{envarsub};
		my $expected	=	$test->{expected};
		$self->logDebug("name", $name);
		$self->logDebug("variable", $variable);

		#### ADD CUSTOM ENVARS
		$self->_addCustomVars($customvars) if defined $customvars;

		#### SET VALUES
		my $envars = $self->envars();
		$self->setEnvarValues($envars, $values);
		
		#### RELOAD VALUES
		$self->getValues();
		
		#### TEST
		my $actual	=	$self->getVar($variable);
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		ok($actual eq $expected, $name);		

		#### UNSET VALUES
		$self->unsetEnvarValues($envars, $values);
	}
}

method setEnvarValues ($envars, $values) {
	$self->logDebug("envars", $envars);
	$self->logDebug("values", $values);
	
	foreach my $envar ( keys %$values ) {
		next if not defined $values->{$envar};
		$self->logDebug("SETTING $envar VALUE", $values->{$envar});
		$ENV{$envars->{$envar}} = $values->{$envar};
	}
}

method unsetEnvarValues ($envars, $values) {
	$self->logDebug("envars", $envars);
	$self->logDebug("values", $values);
	
	foreach my $envar ( keys %$values ) {
		next if not defined $values->{$envar};
		$self->logDebug("UNSETTING $envar VALUE", $values->{$envar});
		$ENV{$envars->{$envar}} = undef;
	}
}

method testEnvarSub ($values, $envars, $parent) {
	$self->logDebug("self", $self);
	$self->logDebug("values", $values);
	$self->logDebug("envars", $envars);
	my $username	=	$self->getVar("username");
	my $project		=	$self->getVar("project");
	my $workflow	=	$self->getVar("workflow");
	my $queue = "$username.$project.$workflow";
	$self->logDebug("queue", $queue);
	
	$values->{queue} = $queue;

	return $self->values($values);
}

method testSetVars {
	diag("setVars");
	
	my $tests = [
		{
			name	=>	"simple",
			variable	=>	"WORKFLOW",
			values	=>	{
				project => "Project1",
				workflow =>	"Workflow1"
			},
			expected	=>	"Workflow1"
		}
	];
	
	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $variable	=	$test->{variable};
		my $values		=	$test->{values};
		my $expected	=	$test->{expected};
		$self->logDebug("name", $name);
		
		$self->values($values);
		$self->setVars();

		my $actual	=	$ENV{$variable};
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		ok($actual eq $expected, $name);		
	}	
}

method testToString {
	diag("toString");
	
	my $tests = [
		{
			name	=>	"simple",
			values	=>	{
				username 	=> 	"testuser",
				project 	=> 	"Project1",
				workflow 	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1"
			},
			customvars 	=>	{
				qmasterport 	=> 	"SGE_MASTER_PORT",
				execdport 		=> 	"SGE_EXECD_PORT",
				sgecell 		=> 	"SGE_CELL",
				sgeroot 		=> 	"SGE_ROOT",
				queue 			=> 	"QUEUE"
			},
			expected	=>	"export PROJECT=Project1; export SGE_CELL=testuser.Project1.Workflow1; export USERNAME=testuser; export WORKFLOW=Workflow1;"
		}
	];
	
	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $values		=	$test->{values};
		my $customvars	=	$test->{customvars};
		my $expected	=	$test->{expected};
		$self->logDebug("name", $name);
		$self->logDebug("values", $values);
		
		#### ADD CUSTOM ENVARS
		$self->_addCustomVars($customvars) if defined $customvars;

		#### SET VALUES
		my $envars = $self->envars();
		$self->logDebug("envars", $envars);
		$self->logDebug("values", $values);
		foreach my $envar ( keys %$values ) {
			$self->logDebug("envar", $envar);
			next if not defined $values->{$envar};
			$self->logDebug("SETTING $envar VALUE", $values->{$envar});
			$ENV{$envars->{$envar}} = $values->{$envar};
		}

		#### RELOAD VALUES
		$self->getValues();

		my $actual	=	$self->toString();
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		ok($actual eq $expected, $name);		
	}	
}





}
