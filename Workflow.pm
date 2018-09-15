use MooseX::Declare;

=head2

	PACKAGE		Engine::Workflow
	
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
				-> 	my $stage = Engine::Stage->new()
					...
					|
					|
					-> $stage->run()
						|
						|
						? DEFINED 'CLUSTER' AND 'SUBMIT'
						|				|
						|				|
						|				YES ->  Engine::Stage::runOnCluster() 
						|
						|
						NO ->  Engine::Stage::runLocally()

=cut

use strict;
use warnings;
use Carp;

class Engine::Workflow {

#### EXTERNAL MODULES
use FindBin qw($Bin);
use Data::Dumper;

##### INTERNAL MODULES	

# Bool
# Int
# String
# Object

sub new {
    my $class = shift;
    my $args	= shift;

    my $modulepath =  $INC{"Engine/Workflow.pm"};
    # print "modulepath: $modulepath\n";
    my ($path) = $modulepath =~ /^(.+?)\/[^\/]+.pm$/; 

    my $scheduler = $args->{conf}->getKey("core:SCHEDULER");
    my $runtype = "Local";
    my $submit = $args->{submit};
    # print "submit: $submit\n" if defined $submit;
    # print "submit: undef\n" if not defined $submit;
    # print "scheduler: $scheduler\n";

    if ( defined $scheduler 
      and $scheduler ne ""
      and $scheduler ne "local"  ) {
      if ( not defined $submit or $submit ne "0" ) {
        $runtype = "Cluster";
      }
    }
    # print "**** *** *** **** Engine::Workflow    runtype: $runtype\n";
 
    my $location    = "$path/$runtype/Workflow.pm";
    $class          = "Engine::" . $runtype . "::Workflow";
    require $location;

    return $class->new( $args );
}
    
  Engine::Workflow->meta->make_immutable(inline_constructor => 0);

}	#### class
