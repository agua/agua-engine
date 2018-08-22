use MooseX::Declare;
use Method::Signatures::Simple;

=head2

PACKAGE		Engine::Logic::Email

PURPOSE

    1. SEND EMAIL VIA GMAIL ACCOUNT TO RECIPIENT
    
    
INPUTS
    
    1. RECIPIENT EMAIL ADDRESS

    2. TITLE/SUBJECT OF EMAIL
    
    3. MESSAGE BODY

OUTPUTS

	1. SENT MESSAGE TO GMAIL ACCOUNT

=cut

use strict;
use warnings;
use Carp;

use FindBin qw($Bin);

class Engine::Logic::Email with (Util::Logger, Util::Main, Table::Main) {

use Email::Send;
use Email::Simple;

#### INTERNAL MODULES
use Conf::Yaml;

# Int
has 'log'		=>  ( isa => 'Int', is => 'rw', default => 4 );
has 'printlog'	=>  ( isa => 'Int', is => 'rw' );

# String
has 'username'  	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'workflow'  	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'project'   	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'modfile'	    => ( isa => 'Str|Undef', is => 'rw' );
has 'inputfiles'	=> ( isa => 'Str|Undef', is => 'rw' );
has 'regex'	=> ( isa => 'Str|Undef', is => 'rw' );
has 'if'	=> ( isa => 'Str|Undef', is => 'rw' );
has 'elsif'	=> ( isa => 'ArrayRef|Undef', is => 'rw' );
has 'else'	=> ( isa => 'Str|Undef', is => 'rw' );

# Object
has 'db'	=> ( isa => 'DBase::MySQL', is => 'rw', required => 0 );
has 'conf' 	=> (
	is =>	'rw',
	isa => 'Conf::Yaml',
	default	=>	sub { Conf::Yaml->new( {} );	}
);

####/////}


method BUILD ($hash) {
	$self->logDebug("");
	#$self->logDebug("self", $self);
	$self->initialise($hash);
}

method initialise ($hash) {	
	$self->logDebug("");
    
	#### SET CONF LOG
	$self->conf()->log($self->log());
	$self->conf()->printlog($self->printlog());	
}

=head2

SUBROUTINE		send

PURPOSE

    1. SEND AN EMAIL TO THE DESIGNATED RECIPIENT
	
		FROM A USER-DESIGNATED OR PRECONFIGURED
		
		GMAIL ACCOUNT
		
INPUTS

    1. ENVIRONMENT VARIABLES: username AND password
	
    2. OPERATIONAL METADATA:
	
			PROJECT, WORKFLOW, STAGENUMBER, STAGE, SAMPLE
			
	3. MESSAGE DATA:
	
			TO, FROM, SUBJECT, MESSAGE

OUTPUTS

	1. EMAIL SENT TO RECIPIENT, ADDRESSED AS FROM 'SENDER'
    
=cut

method send ($hash) {
	my $project		=	$hash->{project};
	my $workflow	=	$hash->{workflow};
	my $stagenumber	=	$hash->{stagenumber};
	my $stage		=	$hash->{stage};
	my $sample		=	$hash->{sample};

	my $username	=	$hash->{username};
	my $password	=	$hash->{password};
	
	my $to			=	$hash->{to};
	my $from		=	$hash->{from};
	my $subject		=	$hash->{subject};
	my $message		=	$hash->{message};
	my $messagefile =	$hash->{messagefile};
	
	$self->logDebug("username", $username);
	$self->logDebug("password", $password);

    if ( defined $messagefile ) {
        $self->logDebug("messagefile", $messagefile);
        $message = $self->getFileContents($messagefile);
    }
    #$self->logDebug("message", $message);
    
	$project = $ENV{'project'} if not defined $project;
	$workflow = $ENV{'workflow'} if not defined $workflow;
	$stagenumber = $ENV{'stagenumber'} if not defined $stagenumber;
	$stage = $ENV{'stage'} if not defined $stage;
	$sample = $ENV{'sample'} if not defined $sample;
	
	$self->logDebug("project", $project);
	$self->logDebug("workflow", $workflow);
	$self->logDebug("stagenumber", $stagenumber);
	$self->logDebug("stage", $stage);
	$self->logDebug("sample", $sample);

	$self->logDebug("to", $to);
	$self->logDebug("from", $from);
	
	$subject =~ s/%USERNAME%/$username/g;
	$subject =~ s/%PROJECT%/$project/g;
	$subject =~ s/%WORKFLOW%/$workflow/g;
	$subject =~ s/%STAGENUMBER%/$stagenumber/g;
	$subject =~ s/%STAGE%/$stage/g;
	$subject =~ s/%SAMPLE%/$sample/g;
	$self->logDebug("subject", $subject);
	
	$message =~ s/%USERNAME%/$username/g;
	$message =~ s/%PROJECT%/$project/g;
	$message =~ s/%WORKFLOW%/$workflow/g;
	$message =~ s/%STAGENUMBER%/$stagenumber/g;
	$message =~ s/%STAGE%/$stage/g;
	$message =~ s/%SAMPLE%/$sample/g;
	$self->logDebug("message", $message);

	#### DATABASE
	$self->setDbh() if not defined $self->table()->db();
	
	my $email = Email::Simple->create(
		header => [
			From    => 	$from,
			To      => 	$to,
			Subject => 	$subject,
		],
		body => $message,
	);
    $email->header_set("From", 'Zito, James <jzito@sapiosciences.com>');
	
	my $sender = Email::Send->new(
		{
			mailer      => 'Gmail',
			mailer_args => [
				username => $username,
				password => $password,
			]
		}
	);
	eval { $sender->send($email) };
	$self->logDebug("Error sending email: $@") if $@;
}


}

