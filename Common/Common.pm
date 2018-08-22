package Engine::Common::Common;
use Moose::Role;

=head2

	PACKAGE		Engine::Common::Common
	
	PURPOSE 	

		O-O MODULE CONTAINING COMMONLY-USED Agua METHODS
		
=cut

#### STRINGS/INTS	
has 'validated'	=> ( isa => 'Int', is => 'rw', default => 0 );
has 'cgi'		=> ( isa => 'Str|Undef', is => 'rw', default => undef );

use strict;
use warnings;
use Carp;

#### EXTERNAL MODULES
use FindBin qw($Bin);
use Data::Dumper;
use File::Path;
use File::Copy;
use File::Remove;
use File::stat;
use JSON;

with 'Util::Timer';

# with 'Util::Main';

with 'Package::Main';

# with 'Table::Main';
# with 'Table::Parameter';
# with 'Table::App';
# with 'Table::Package';
# with 'Table::Project';
# with 'Table::Sample';
# with 'Table::Stage';
# with 'Table::Workflow';

with 'Web::File::Cache';
with 'Web::Base';
with 'Web::History';
with 'Web::Login';
with 'Web::Transport::Main';
with 'Web::Report';
with 'Web::Request';
with 'Web::View::Main';

with 'Web::Cloud::Ami';
with 'Web::Cloud::Aws';
with 'Web::Cloud::Cluster';
with 'Web::Cloud::Main';
with 'Web::Cloud::Hub';

with 'Web::Group::Access';
with 'Web::Group::Main';
with 'Web::Group::Source';
with 'Web::Group::Privileges';
with 'Web::Group::Shared';
with 'Web::Group::Sharing';
with 'Web::Group::User';

with 'Engine::Common::Ssh';
with 'Engine::Common::SGE';




1;