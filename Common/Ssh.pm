package Engine::Common::Ssh;
use Moose::Role;
use Method::Signatures::Simple;

# use Util::Ssh;

=head2

	PACKAGE		Engine::Common::Ssh
	
	PURPOSE
	
		SSH METHODS

=cut

use Data::Dumper;
use File::Path;

has 'keyname'	=> ( isa => 'Str|Undef', is => 'rw', required	=>	0	);

has 'keypairfile'	=> ( is  => 'rw', 'isa' => 'Str|Undef', required	=>	0	);

method _setSsh ( $username, $hostname, $keyfile ) {
	$self->logDebug("username", $username);
	$self->logDebug("hostname", $hostname);
	$self->logDebug("keyfile", $keyfile);
	
	my $ssh = Util::Ssh->new(
		remoteuser	=> 	$username,
		keyfile			=> 	$keyfile,
		remotehost	=>	$hostname
	);
	$self->ssh($ssh);
	
	return $ssh;
}

#### SET SSH COMMAND IF KEYPAIRFILE, ETC. ARE DEFINED
method setKeypairFile ( $username) {
	$self->logCaller("username: $username");

	$username = $self->username() if not defined $username;
	$self->logError("username not defined") and exit if not defined $username;

	my $keyname 	= 	"$username-key";
	my $conf 		= 	$self->conf();
	#$self->logDebug("conf", $conf);
	my $userdir 	= 	$conf->getKey("agua:USERDIR");
	$self->logCaller("userdir not defined") and exit if not defined $userdir;

	my $keypairfile = "$userdir/$username/.starcluster/id_rsa-$keyname";

	my $adminkey 	= 	$self->getAdminKey($username);
	$self->logDebug("adminkey", $adminkey);
	return if not defined $adminkey;
	my $configdir = "$userdir/$username/.starcluster";
	if ( $adminkey ) {
		my $adminuser = $self->conf()->getKey("core:ADMINUSER");
		$self->logDebug("adminuser", $adminuser);
		my $keyname = "$adminuser-key";
		$keypairfile = "$userdir/$adminuser/.starcluster/id_rsa-$keyname";
	}
	$self->keypairfile($keypairfile);
	
	return $keypairfile;
}

method getAdminKey ($username) { 	
	$self->logCaller("username", $username);
	$self->logDebug("username not defined") and return if not defined $username;

	return $self->adminkey() if $self->can('adminkey') and defined $self->adminkey();
	
	my $adminkey_names = $self->conf()->getKey("aws:ADMINKEY");
 	#$self->logDebug("adminkey_names", $adminkey_names);
	$adminkey_names = '' if not defined $adminkey_names;
	my @names = split ",", $adminkey_names;
	my $adminkey = 0;
	foreach my $name ( @names ) {
	 	#$self->logDebug("name", $name);
		if ( $name eq $username )	{	return $adminkey = 1;	}
	}

	$self->adminkey($adminkey) if $self->can('adminkey');
	
	return $adminkey;
}

1;