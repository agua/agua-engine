use MooseX::Declare;

class Test::Engine::Logic::Email with (Test::Common, Agua::Common) extends Engine::Logic::Email {

use Test::More;
use FindBin qw($Bin);

#use DBase::Factory;
#use Engine::Workflow;
use GMail::Checker;

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

method testSend {
	diag("send");

	my $password = $ENV{'password'};
	$self->logDebug("password", $password);
	my $username = $ENV{'username'};
	$self->logDebug("username", $username);
	
	my $tests = [
		{
			name		=>	"simple",
			
			project		=>	"Project1",
			workflow	=>	"Workflow1",
			stagenumber	=>	2,
			stage		=>	"fastqcfork",
			sample		=>	"NA12878",
			
			username 	=>	$username,
			password	=>	$password,
			from		=>	"$username\@gmail.com",
			to 			=>	"$username\@gmail.com",
			subject		=>	qq{Sample %SAMPLE% failed to pass FastQC [%USERNAME%:%PROJECT%:%WORKFLOW%|continue:%STAGENUMBER% %STAGE%]},
			message		=>	qq{Processing of sample %SAMPLE% in workflow '%PROJECT%:%WORKFLOW%' stopped at stage %STAGENUMBER%: %STAGE%.

Check the following files to troubleshoot:

Filetype  Location
STDOUT    /home/%USERNAME%/agua/%PROJECT%/%WORKFLOW%/%STAGE%/stdout/%SAMPLE%.*.stdout
STDERR    /home/%USERNAME%/agua/%PROJECT%/%WORKFLOW%/%STAGE%/stdout/%SAMPLE%.*.stderr
OUTPUT    /home/%USERNAME%/agua/%PROJECT%/%WORKFLOW%/%STAGE%/%SAMPLE%.fastqc.out.

If you want to continue processing sample %SAMPLE%, reply to this message with the word "Continue" as the first line of the message body}
		}
	];

	foreach my $test ( @$tests ) {
		my $name		=	$test->{name};
		my $username	=	$test->{username};
		my $password	=	$test->{password};

		$self->logDebug("name", $name);
			
		#### GET EMAIL COUNT
		my $gwrapper = new GMail::Checker(USERNAME => $username, PASSWORD => $password);
		$gwrapper->login($username,$password);
		my ($messagecount, $size) = $gwrapper->get_msg_nb_size();
		print "BEFORE messagecount: $messagecount\n";

	
		$self->logDebug("BEFORE self->send()");
		$self->send({
			username  	=>  $username,
			password	=>	$password,
			project  	=>  $test->{project},
			workflow  	=>  $test->{workflow},
			stagenumber =>  $test->{stagenumber},
			stage       =>  $test->{stage},
			sample      =>  $test->{sample},
		
			to          =>  $test->{to},
			from        =>  $test->{from},
			subject     =>  $test->{subject},
			message     =>  $test->{message}
		});
		$self->logDebug("AFTER self->send()");

		sleep(2);
		$gwrapper = new GMail::Checker(USERNAME => $username, PASSWORD => $password);
		$gwrapper->login($username,$password);
		my $newmessagecount = 0;
		($newmessagecount, $size) = $gwrapper->get_msg_nb_size();
		print "AFTER messagecount: $messagecount\n";

		my @msg = $gwrapper->get_msg(MSG	=> $newmessagecount);
		#$self->logDebug("msg[0]", $msg[0]);
		#print $msg[0]->{content}, "\n";
		#print $msg[0]->{body};

		my $receivedbody = $msg[0]->{body};
		$self->logDebug("receivedbody", $receivedbody);
		
		ok($newmessagecount == $messagecount + 1, $name);
	}
	$self->logDebug("completed");
}

method testReceive {
	diag("recieive");

	$self->logDebug("");

}



}   #### Test::Common::Cluster