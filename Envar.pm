use MooseX::Declare;

use strict;
use warnings;

class Envar with Util::Logger {

#### EXTERNAL MODULES
use Test::More;
use FindBin qw($Bin);

#### INTERNAL MODULES
# use DBase::MySQL;
use Conf::Yaml;

# Int
has 'log'		=>  ( isa => 'Int', is => 'rw', default => 2 );
has 'printlog'	=>  ( isa => 'Int', is => 'rw', default => 2 );

# String
has 'sessionid'     => ( isa => 'Str|Undef', is => 'rw' );

# Hash/Array
has 'envars'	=>	( isa => 'HashRef|Undef', is => 'rw', lazy => 1, builder => "_getEnvarHash" );
has 'values'	=>	( isa => 'HashRef|Undef', is => 'rw', default => sub { {} } );
has 'customvars'=>	( isa => 'HashRef|Undef', is => 'rw' );

# Subroutine
has 'envarsub'	=>	( isa => 'Maybe', is => 'rw', default => undef );

# Object
has 'parent'	=>	( isa => 'Maybe', is => 'rw', default => undef );
has 'db'		=> ( isa => 'Any', is => 'rw', required => 0 );
has 'conf'	=> ( isa => 'Conf::Yaml', is => 'rw', lazy => 1, builder => "setConf" );

#####/////}}}}}

method BUILD ($args) {
	if ( defined $args ) {
		foreach my $arg ( $args ) {
			$self->$arg($args->{$arg}) if $self->can($arg);
		}
	}

	$self->initialise();	
}

method initialise {
	$self->getValues();
}

method _addCustomVars ($customvars) {
	my $envars 	= $self->envars();
	if ( defined $customvars ) {
		foreach my $customvar ( keys %$customvars ) {
			$envars->{$customvar}	=	$customvars->{$customvar};
		}
	}

	return $self->envars($envars);
}

method _runEnvarSub ($subroutine, $envars, $values, $parent) {
	$self->logDebug("subroutine", $subroutine);
	return $values if not defined $subroutine;

	return &$subroutine($self, $envars, $values, $parent);
}

method getValues {
	$self->logDebug("");
	my $envars 	=	$self->envars();
	#$self->logDebug("envars", $envars);
	$envars = $self->_addCustomVars($self->customvars()) if defined $self->customvars();
	#$self->logDebug("AFTER _addCustomVars envars", $envars);
	my $values 	=	$self->values();
	foreach my $variable ( keys %$envars ) {
		my $envar = $envars->{$variable};
		#$self->logDebug("$envar variable", $variable);
		my $value =	$ENV{$envar} if defined $ENV{$envar};
		#$self->logDebug("value", $value);
		$value	=	$self->$variable() if $self->can($variable) and defined $self->$variable();
		next if not defined $value;
		#$self->logDebug("POST-ATTRIBUTE $variable value", $value);

		$values->{$variable} = $value;
	}
	$self->logDebug("FINAL values", $values);

	my $parent	=	$self->parent();
	$values = $self->_runEnvarSub($self->envarsub(), $envars, $values, $parent) if defined $self->envarsub();	
	
	return $self->values($values);
}

method setVars {
	my $envars 	=	$self->envars();
	my $values 	=	$self->values();

	foreach my $envar ( keys %$values ) {
		my $variable = $envars->{$envar};
		#$self->logDebug("$envar variable", $variable);
		$ENV{$variable} = $values->{$envar};
	}
}

method getVar ($envar) {
	$self->logDebug("envar", $envar);
	my $values = $self->values();
	$self->logDebug("values", $values);
	
	if ( $values->{$envar} ) {
		return $values->{$envar};
	}
}

method setVar ($envar, $value) {
	my $values 	=	$self->values();
	$values->{$envar} = $value;
	
	$self->values($values);
}

method toString {
	my $string = "";
	my $envars 	= $self->envars();
	my $values 	= $self->values();
	$self->logDebug("values", $values);
	my @keys = keys %$values;
	@keys = sort @keys;
	foreach my $envar ( @keys ) {
		$string 	.= "export " . $envars->{$envar} . "=" . $values->{$envar} . "; ";
	}
	$string =~ s/\s+$//;
	$self->logDebug("RETURNING string", $string);
	
	return $string;
}

method _getEnvarHash {
	return {
		username		=>	"USERNAME",
		project			=>	"PROJECT",
		workflow		=>	"WORKFLOW",
		workflownumber	=>	"WORKFLOWNUMBER",
		stage			=>	"STAGE",
		stagenumber		=>	"STAGENUMBER",
		sample			=>	"SAMPLE",
		sessionid		=>	"SESSION_ID"
	}
}


#	my $parent		=	$hash->{parent};
#	my $username	=	$hash->{username};
#	my $cluster		=	$hash->{cluster};
#	
#	return $parent->envars() if $parent->can('envars') and $parent->envars();
#
#	$username 		=	$self->username() if not defined $username;
#	$cluster 		=	$self->cluster() if not defined $cluster;
#
#	my $sessionid 	=	$self->sessionid(); 
#	my $qmasterport;
#	my $execdport;
#	my $sgeroot;
#	my $queue;
#	my $project		=	$self->project();
#	my $workflow	=	$self->workflow();
#	my $sgecell 	= 	$cluster if defined $cluster;
#	$sgecell = '' if not defined $sgecell;
#	$self->logNote("sgecell", $sgecell);
#	
#	#### IF THE INITIAL (PARENT) WORKFLOW JOB WAS RUN LOCALLY MUST PICK UP THE SGE 
#	#### ENVIRONMENT VARIABLES FROM THE SHELL IN ORDER TO IDENTIFY WHERE TO SUBMIT JOBS TO
#	$self->logNote("Retrieving environment variables from shell");
#	$sessionid		=	$ENV{'SESSION_ID'} if not defined $sessionid or not $sessionid;
#	$username 		= 	$ENV{'USERNAME'} if defined $ENV{'USERNAME'} and (not defined $username or not $username) and $ENV{'USERNAME'};
#	$cluster 		= 	$ENV{'CLUSTER'} if defined $ENV{'CLUSTER'};
#	$qmasterport 	= 	$ENV{'SGE_MASTER_PORT'};
#	$execdport 		= 	$ENV{'SGE_EXECD_PORT'};
#	$sgecell 		= 	$ENV{'SGE_CELL'} if not defined $sgecell;
#	$sgeroot 		= 	$ENV{'SGE_ROOT'};
#	$queue 			= 	$ENV{'QUEUE'};
#	$project 		= 	$ENV{'PROJECT'} if not defined $project or not $project;
#	$workflow 		= 	$ENV{'WORKFLOW'} if not defined $workflow or not $workflow;
#	$sgeroot 		=	$self->conf()->getKey("cluster", 'SGEROOT') if not defined $sgeroot;
#
#	#### SET USERNAME AND CLUSTER IF NOT DEFINED
#	$self->username($username) if not $self->username();
#	$self->sessionid($sessionid) if not $self->sessionid();
#	$self->cluster($sgecell) if not $self->cluster();
#	$self->queue($queue) if not $self->queue();
#	
#	#### THIS JOB IS THE INITIAL (PARENT) WORKFLOW JOB LAUNCHED BY THE SYSTEM.
#	#### IT RETRIEVES THE SGE PORT VARIABLES FROM THE DB
#	if ( not defined $qmasterport
#		or (
#			defined $username and $username
#			and defined $sgecell and $sgecell
#			and defined $self->table()->db()
#			and defined $self->table()->db()->dbh()			
#		)
#	) {
#		$self->logNote("Retrieving environment variables from database");
#		my $query = qq{SELECT qmasterport
#FROM clustervars
#WHERE username = '$username'
#AND cluster = '$sgecell'};
#		$self->logNote("$query");
#		$qmasterport 	= 	$self->table()->db()->query($query);
#		$execdport 		= 	$qmasterport + 1 if defined $qmasterport;
#		$self->logNote("qmasterport", $qmasterport);
#		$self->logNote("execdport", $execdport);
#	}
#
#	#### IF project AND workflow ARE NOT DEFINED, USE SLOTS IF FILLED
#	$project = $self->project() if $self->can('project') and $self->project();
#	$workflow = $self->workflow() if $self->can('workflow') and $self->workflow();
#
#	$self->logNote("BEFORE queue = self->queueName(username, project, workflow)");
#	$self->logNote("project", $project) if defined $project;
#	$self->logNote("workflow", $workflow) if defined $workflow;
#	$queue = $self->queueName($username, $project, $workflow) if defined $project and defined $workflow;
#	$self->queue($queue) if defined $queue and not $self->queue();
#
#	$self->logNote("queue", $queue) if defined $queue;
#	$self->logNote("username", $username) if defined $username;
#	$self->logNote("qmasterport", $qmasterport) if defined $qmasterport;
#	$self->logNote("execdport", $execdport) if defined $execdport;
#	$self->logNote("sgeroot", $sgeroot) if defined $sgeroot;
#	$self->logNote("queue", $queue) if defined $queue;
#	$self->logNote("queue not defined") if not defined $queue;
#	
#	my $envars = {};
#	$envars->{qmasterport} 	= $qmasterport;
#	$envars->{execdport} 	= $execdport;
#	$envars->{sgeroot} 		= $sgeroot;
#	$envars->{sgecell} 		= $sgecell;
#	$envars->{username} 	= $username;	
#	$envars->{queue} 		= $queue;	
#	$envars->{project} 		= $project;	
#	$envars->{workflow} 	= $workflow;	
#	$envars->{sessionid} 	= $sessionid if defined $sessionid;	
#	$envars->{tostring} 	= "export SGE_QMASTER_PORT=$qmasterport; " if defined $qmasterport;
#	$envars->{tostring} 	.= "export SGE_EXECD_PORT=$execdport; " if defined $execdport;
#	$envars->{tostring} 	.= "export SGE_ROOT=$sgeroot; " if defined $sgeroot;
#	$envars->{tostring} 	.= "export SGE_CELL=$sgecell; " if defined $sgecell;
#	$envars->{tostring} 	.= "export USERNAME=$username; " if defined $username;
#	$envars->{tostring} 	.= "export QUEUE=$queue; " if defined $queue;
#	$envars->{tostring} 	.= "export PROJECT=$project; " if defined $project;
#	$envars->{tostring} 	.= "export WORKFLOW=$workflow; " if defined $workflow;
#	$envars->{tostring} 	.= "export SESSION_ID=$sessionid; " if defined $sessionid;
#	$self->logNote("envars->{tostring}", $envars->{tostring});
#
#	$self->envars($envars);
#
#	return $envars;


}

