use MooseX::Declare;

=head2

PACKAGE		Engine::Engine::Logic::Fork

PURPOSE

    1. SELECT ONE OR MORE WORKFLOW PATHS BASED ON ONE OR BOTH OF THE FOLLOWING:
    
        - CONTENTS OF INPUT FILE (USING --inputfile OPTION)
    
        - CONTENTS OF DATABASE AND OTHER DATA (USING --modfile OPTION)

    2. SELECT BRANCH BY:

        - INDEX FOUND IN FIRST LINE OF INPUT FILE (DEFAULT, USES --inputfile OPTION)

        - INSTRUCTIONS LOADED DYNAMICALLY FROM *.pm FILE (--modfile OPTION)

        - STAGES AS ARGUMENTS:
        
            --if
            --elsif
            --else
            
        
        - MULTIPLE --regex AND --branch OPTIONS
        
            WHERE --branch IS A STRING OF FORMAT 'Project:Workflow:StageNumber:StageName'

    
=cut

#### USE LIB FOR INHERITANCE
use FindBin::Real;
use lib FindBin::Real::Bin() . "/lib";
use Data::Dumper;

class Engine::Engine::Logic::Fork with Util::Logger {

#### USE LIB
use FindBin::Real;
use lib FindBin::Real::Bin() . "/lib";

#### INTERNAL MODULES
use DBase::Factory;
use Conf::Yaml;

# Int
has 'log'		    =>  ( isa => 'Int', is => 'rw', default => 4 );
has 'printlog'	    =>  ( isa => 'Int', is => 'rw' );

# String
has 'username'  	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'workflow'  	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'project'   	=>  ( isa => 'Str|Undef', is => 'rw' );
has 'inputfiles'	=> ( isa => 'Str|Undef', is => 'rw' );
has 'modfile'	    => ( isa => 'Str|Undef', is => 'rw' );
has 'regex'	        => ( isa => 'Str|Undef', is => 'rw' );
has 'if'	        => ( isa => 'Str|Undef', is => 'rw' );
has 'elsif'	        => ( isa => 'ArrayRef|Undef', is => 'rw' );
has 'else'	        => ( isa => 'Str|Undef', is => 'rw' );
has 'outputfile'	=> ( isa => 'Str|Undef', is => 'rw' );

# Object
has 'db'	        => ( isa => 'DBase::MySQL', is => 'rw', required => 0 );
has 'conf' 	        => (
	is =>	'rw',
	isa => 'Conf::Yaml',
	default	=>	sub { Conf::Yaml->new( {} );	}
);


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

SUBROUTINE		branch

PURPOSE

    1. SELECT ONE OR MORE WORKFLOW PATHS BASED ON

		AN INPUT FILE, OR INSTRUCTIONS IN A
		
		DYNAMICALLY LOADED PERL MODULE FILE,
		
		OR BOTH
		
INPUTS

    1. (OPTIONAL) INPUT FILE
	
		DEFAULT FORMAT: FIRST LINE CONTAINS A
		
		ZER0-INDEXED INTEGER DEFINING WHICH
		
		BRANCH TO SELECT. E.G.:
		
			FILE CONTENTS: 0
			
			BRANCHES: 'if, else'
			
			'0' SIGNIFIES THE 'IF' BRANCH AND '1'
			
			SIGNIFIES THE 'ELSE' BRANCH SO, IN
			
			THIS CASE THE 'IF' BRANCH IS SELECTED
			

			FILE CONTENTS: 1
			
			BRANCHES: 'if, else'
			
			THE 'ELSE' BRANCH IS SELECTED
		
		
			FILE CONTENTS: 2
			
			BRANCHES: 'if, elsif, elsif, else'
			
			THE SECOND 'ELSIF' BRANCH IS SELECTED

    
	2. (OPTIONAL) DYNAMICALLY LOADED PERL MODULE


	TO DO:

    3. (OPTIONAL) STAGES AS ARGUMENTS:
        
            --if
            --elsif
            --else

		WHERE if/elsif/else ARE STAGE NUMBERS

    4. (OPTIONAL) REGEX FOR PARSING INPUT FILE

OUTPUTS

	1. UPDATED successor FIELD IN fork STAGE
	
		ENTRY IN stage TABLE
	
	2. UPDATED ancestor ENTRIES IN stage TABLE
		
		FOR if/elsif/else STAGES:
		
			ancestor = FORK STAGE IF SELECTED
			
			ancestor = NULL IF NOT SELECTED
		
	3. UPDATE STATUS FOR NON-SELECTED STAGES TO
	
		'skip'

NOTES
    1. MAIN METHOD: selectBranch LOADS MODULE FILE IF
    
        AVAILABLE AND THEN CALLS parseInputfiles
        
    2. parseInputfiles MAKES THE FOLLOWING CALLS:
    
        - selectIndex TO GET 'if', 'elsif' or 'else' INDEX
        
        - printOutputfile TO PRINT RESULT DETAILS TO FILE
        
        - setValues TO UPDATE successor/ancestor IN stage TABLE

=cut

method selectBranch {
	$self->logDebug("self->username()", $self->username());
	$self->logDebug("self->project()", $self->project());
	$self->logDebug("self->workflow()", $self->workflow());
	$self->logDebug("self->inputfiles()", $self->inputfiles());
	
	$self->logDebug("self->modfile()", $self->modfile());
	$self->logDebug("self->regex()", $self->regex());

	#### DATABASE
	$self->setDbh() if not defined $self->table()->db();
	
	#### MODFILE
	$self->loadModfile($self->modfile()) if defined $self->modfile();
	
	#### INPUTFILE
	$self->parseInputfiles($self->username(), $self->project(), $self->workflow(), $self->inputfiles(), $self->if(), $self->else(), $self->elsif(), $self->outputfile()) if defined $self->inputfiles();
	
	#### OPTIONAL POST-PROCESS
	$self->postProcess() if $self->can("postProcess");		
}

method parseInputfiles ($username, $project, $workflow, $inputfiles, $if, $else, $elsif, $outputfile) {
	$self->logDebug("inputfiles", $inputfiles);
	$self->logDebug("if", $if);
	$self->logDebug("else", $else);
	$self->logDebug("elsif", $elsif);
	my $branches = [];
	push @$branches, $self->if();
	@$branches = (@$branches, $elsif) if defined $elsif;
	push @$branches, $self->else();
	$self->logDebug("branches", $branches);

    my ($index, $results) = $self->selectIndex($inputfiles);
    
    $self->printOutputfile($outputfile, $results) if defined $results;
    
	$self->setValues($username, $project, $workflow, $branches, $index) if defined $if;
}

method printOutputfile ($outputfile, $results) {
  $self->logDebug("outputfile", $outputfile);
  open(OUT, ">", $outputfile) or die "Can't open outputfile: $outputfile\n";

  my $fields = [
      "metric",
      "rulename",
      "outcome",
      "threshold",
      "operator",
      "actual",
      "result"
  ];

  foreach my $result ( @$results ) {
      $self->logDebug("result", $result);
      my $output = "";
      foreach my $field ( @$fields ) {
          my $value = $result->{result}->{$field};
          $output .= "$value\t" if defined $value;
      }
      $output =~ s/\t$/\n/;
      $self->logDebug("output", $output);
      print OUT $output;
  }   
}

method selectIndex ($inputfile) {
	if ( not -f $inputfile ) {
		return 1;
	}
	my $contents	=	$self->getFileContents($inputfile);
	$self->logDebug("contents", $contents);

    if ( $contents	=~ /^\s*(\S+)/ ) {
	    return $1;    	
    }
    else {
    	return undef;
    }
}

method setValues ($username, $project, $workflow, $branches, $index) {
	$self->logDebug("username", $username);
	$self->logDebug("project", $project);
	$self->logDebug("workflow", $workflow);
	
	#### SET WORKFLOW IDENTIFIERS FROM ENVARS.
	#### ELSE, USE USER-PROVIDED VALUES
	$username	=	$ENV{'USERNAME'} if defined $ENV{'USERNAME'};
	$project	=	$ENV{'PROJECT'} if defined $ENV{'PROJECT'};
	$workflow	=	$ENV{'WORKFLOW'} if defined $ENV{'WORKFLOW'};
	my $stagenumber	=	$ENV{'STAGENUMBER'};
	$self->logDebug("project", $project);
	$self->logDebug("stagenumber", $stagenumber);
	$self->logDebug("index", $index);

	my $ancestor	=	$stagenumber;
	$self->logDebug("ancestor", $ancestor);
	my $successorstring	=	$$branches[$index];
	my @successors = split ",", $successorstring;
	my $successor = @successors[0]; 
	$self->logDebug("successor", $successor);
	my $query = qq{UPDATE stage
SET successor='$successor', status='completed'
WHERE username='$username'
AND project='$project'
AND workflow='$workflow'
AND number='$ancestor'
};
	$self->logDebug("query", $query);
	my $success	=	$self->table()->db()->do($query);
	$self->logDebug("success", $success);

	for ( my $i = 0; $i < @$branches; $i++) {
		my $numberstring = $$branches[$i];

		if ( $i == $index ) {
			my $currentancestor = $ancestor;
			my @numbers = split ",", $numberstring;
			for (my $i = 0; $i < @numbers; $i++) {
				my $number = $numbers[$i];
				my $successor = "";
				if ( $i < scalar(@numbers) - 1 ) {
					$successor = ", successor=" . $numbers[$i + 1];
				}
				my $query = qq{UPDATE stage
	SET ancestor='$currentancestor', status='waiting' $successor
	WHERE username='$username'
	AND project='$project'
	AND workflow='$workflow'
	AND number='$number'
	};
				$self->logDebug("query", $query);
				my $success	=	$self->table()->db()->do($query);
				$self->logDebug("success", $success);

				$currentancestor = $number;
			}
		}
		else {
			my @numbers = split ",", $numberstring;
			foreach my $number ( @numbers ) {
				my $query = qq{UPDATE stage
SET ancestor=NULL, successor=NULL, status='skip'
WHERE username='$username'
AND project='$project'
AND workflow='$workflow'
AND number='$number'
};
				$self->logDebug("query", $query);
				my $success	=	$self->table()->db()->do($query);
				$self->logDebug("success", $success);
			}
		}
	}
}

method loadModfile ($modfile) {
	$self->logDebug("modfile", $modfile);

	return if not defined $modfile;
	
	my ($filedir, $filename)	=	$modfile	=~ /^(.+?)\/Logic\/Fork\/([^\/]+)$/;
	$self->logDebug("filedir", $filedir);
	$self->logDebug("filename", $filename);

	my ($modulename)	=	$filename	=~	/^(.+)\.pm/;
	$modulename = "Engine::Logic::Fork::$modulename";
	$self->logDebug("modulename", $modulename);

	if ( -f $modfile ) {
		$self->logDebug("Found modulefile: $modfile");
		$self->logDebug("Doing require $modulename");
		unshift @INC, $filedir;
		my ($olddir) = `pwd` =~ /^(\S+)/;
		# $self->logDebug("olddir", $olddir);
		chdir($filedir);
		eval "require $modulename";
		
		Moose::Util::apply_all_roles($self, $modulename);
	}
	else {
		$self->logDebug("\nCan't find modulefile: $modfile\n");
		print "Engine::Logic::Fork::loadMofile    Can't find modfile: $modfile\n";
		exit;
	}
}


} ## class