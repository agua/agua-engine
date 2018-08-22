use Moose::Util::TypeConstraints;
use MooseX::Declare;
use Method::Signatures::Modifiers;

class Test::Envar extends Envar with Util::Logger {

#### EXTERNAL
use Test::More;
use FindBin qw($Bin);

#### INTERNAL
use StarCluster::Main;
use Engine::Cluster::Monitor::SGE;
use Engine::Stage;
use Web::View::Main;

#####/////}}}}}

method BUILD ($args) {
	if ( defined $args ) {
		foreach my $arg ( $args ) {
			$self->$arg($args->{$arg}) if $self->can($arg);
		}
	}
	
	$self->initialise();
}

method testStarCluster {
	diag("StarCluster");
	
	my $tests = [
		{
			name		=>	"customVars",
			variable	=>	"sgecell",
			values		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1",
				queue		=>	"testuser.Project1.Workflow1",
				qmasterport	=>	63231,
				execdport	=>	63232
			},
			expected		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1",
				queue		=>	"testuser.Project1.Workflow1",
				qmasterport	=>	"63231",
				execdport	=>	"63232",
				sgeroot		=>	"/opt/sge6",
				cluster	=>	"testuser.Project1.Workflow1"
			}
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

		my $object = StarCluster::Main->new({
			conf		=>	$self->conf(),
			log			=>	$self->log(),
			printlog	=>	$self->printlog()
		});
		$self->logDebug("");

		no warnings;
		*StarCluster::Main::getQueueMasterPort = sub {
			#print "OVERRIDE getQueueMasterPort subroutine\n";
			return 63231;
		};
		use warnings;
		
		#### SET VALUES
		$self->logDebug("BEFORE object->envar()");
		my $envar = $object->envar();
		$self->logDebug("AFTER object->envar()");
		$self->logDebug("envar: $envar");
		
		#### SET VALUES
		my $envars = $envar->envars();
		$self->setEnvarValues($envars, $values);

		#### RELOAD VALUES
		$envar->getValues();
		
		#### TEST
		my $actual	=	$envar->values();
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		is_deeply($actual, $expected, $name);		

		#### UNSET VALUES
		$self->unsetEnvarValues($envars, $values);
	}
}

method testMonitorSge {
	diag("monitorSge");
	
	my $tests = [
		{
			name		=>	"customVars",
			variable	=>	"sgecell",
			values		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1",
				queue		=>	"testuser.Project1.Workflow1",
				qmasterport	=>	63231,
				execdport	=>	63232
			},
			expected		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1",
				queue		=>	"testuser.Project1.Workflow1",
				qmasterport	=>	"63231",
				execdport	=>	"63232",
				sgeroot		=>	"/opt/sge6",
				cluster	=>	"testuser.Project1.Workflow1"
			}
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

		my $object = Engine::Cluster::Monitor::SGE->new({
			conf		=>	$self->conf(),
			log			=>	$self->log(),
			printlog	=>	$self->printlog()
		});
		$self->logDebug("");

		no warnings;
		*Engine::Cluster::Monitor::SGE::getQueueMasterPort = sub {
			#print "OVERRIDE getQueueMasterPort subroutine\n";
			return 63231;
		};
		use warnings;

		#### SET VALUES
		$self->logDebug("BEFORE object->envar()");
		my $envar = $object->envar();
		$self->logDebug("AFTER object->envar()");
		$self->logDebug("envar: $envar");
		
		#### SET VALUES
		my $envars = $envar->envars();
		$self->setEnvarValues($envars, $values);

		#### RELOAD VALUES
		$envar->getValues();
		
		#### TEST
		my $actual	=	$envar->values();
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		is_deeply($actual, $expected, $name);		

		#### UNSET VALUES
		$self->unsetEnvarValues($envars, $values);
	}
}

method testStage {
	diag("Stage");
	
	my $tests = [
		{
			name		=>	"customVars",
			variable	=>	"sgecell",
			values		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1",
				queue		=>	"testuser.Project1.Workflow1",
				qmasterport	=>	63231,
				execdport	=>	63232
			},
			expected		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1",
				sgecell		=>	"testuser.Project1.Workflow1",
				queue		=>	"testuser.Project1.Workflow1",
				qmasterport	=>	"63231",
				execdport	=>	"63232",
				sgeroot		=>	"/opt/sge6",
				cluster	=>	"testuser.Project1.Workflow1"
			}
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

		my $object = Engine::Stage->new({
			username  	=> "",
			workflow  	=> "",
			project   	=> "",
			name   		=> "",
			queue		=> "",
			outputdir	=> "",
			scriptfile	=> "",
			installdir  => "",
			version   	=> "",
			stageparameters   	=> [],
			
			conf		=>	$self->conf(),
			log			=>	$self->log(),
			printlog	=>	$self->printlog()
		});
		$self->logDebug("");

		no warnings;
		*Engine::Stage::getQueueMasterPort = sub {
			#print "OVERRIDE getQueueMasterPort subroutine\n";
			return 63231;
		};
		use warnings;

		#### SET VALUES
		$self->logDebug("BEFORE object->envar()");
		my $envar = $object->envar();
		$self->logDebug("AFTER object->envar()");
		$self->logDebug("envar: $envar");
		
		#### SET VALUES
		my $envars = $envar->envars();
		$self->setEnvarValues($envars, $values);

		#### RELOAD VALUES
		$envar->getValues();
		
		#### TEST
		my $actual	=	$envar->values();
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		is_deeply($actual, $expected, $name);		

		#### UNSET VALUES
		$self->unsetEnvarValues($envars, $values);
	}
}

method testView {
	diag("View");
	
	my $tests = [
		{
			name		=>	"customVars",
			variable	=>	"sgecell",
			values		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1"
			},
			expected		=>	{
				username	=>	"testuser",
				project 	=>	"Project1",
				workflow	=>	"Workflow1"
			}
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

		my $object = Web::View::Main->new({
			username  	=> "",
			workflow  	=> "",
			project   	=> "",
			name   		=> "",
			queue		=> "",
			outputdir	=> "",
			scriptfile	=> "",
			installdir  => "",
			version   	=> "",
			stageparameters   	=> [],
			
			conf		=>	$self->conf(),
			log			=>	$self->log(),
			printlog	=>	$self->printlog()
		});
		$self->logDebug("");

		no warnings;
		*Web::View::Main::getQueueMasterPort = sub {
			#print "OVERRIDE getQueueMasterPort subroutine\n";
			return 63231;
		};
		use warnings;

		#### SET VALUES
		$self->logDebug("BEFORE object->envar()");
		my $envar = $object->envar();
		$self->logDebug("AFTER object->envar()");
		$self->logDebug("envar: $envar");
		
		#### SET VALUES
		my $envars = $envar->envars();
		$self->setEnvarValues($envars, $values);

		#### RELOAD VALUES
		$envar->getValues();
		
		#### TEST
		my $actual	=	$envar->values();
		$self->logDebug("actual", $actual);
		$self->logDebug("expected", $expected);
		
		is_deeply($actual, $expected, $name);		

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


}	####	Agua::Login::Common
