#!/usr/bin/perl -w
use strict;
use LWP::UserAgent;
use HTTP::Cookies;
use JSON;

my $server = "http://localhost:2480/";

my $odb = myOrientDb->new( 
		server => $server,
		username => "root",
		password => "root",
		debug => 1
	);

$odb->connect(
		database => "demo",
	);


$odb->beginTransaction();
$odb->appendOperation(
		type => 'CREATE',
		class => 'Prova',
		name => 'ProvaX'
	);
$odb->appendOperation(
		type => 'SCRIPT',
		script => 'Insert into Prova (name) values ("ok")',
	);
$odb->appendOperation(
		type => 'CREATE',
		class => 'Prova',
		name => 'ProvaY'
	);
$odb->commit();












# =======================================================
# myOrientDB REST API
# Francesco Sirocco
# =======================================================
# use LWP::UserAgent;
# use HTTP::Cookies;
# use JSON;
# use HTTP::Headers::ActionPack::Authorization::Basic;
# =======================================================


package myOrientDb;

# =======================================================
# Public methods
# =======================================================

sub new(){
	my ($class, %param) = @_;
	my $self = {};
	foreach my $k (keys %param){
		$self->{$k} = $param{$k};
	}
	bless $self, $class;

	$self->_needed(%param, required => "username password server");

	my $ua = LWP::UserAgent->new(
	);
	$ua->default_header('Accept-Encoding' 	=> 'gzip,deflate');
	$ua->default_header('Content-Length'	=> 0);

	my $cookie_jar = HTTP::Cookies->new(
		file     => "./cookies.lwp",
		ignore_discard => 1,
		autosave => 1,
	);
	$ua->cookie_jar( $cookie_jar );	
	$self->{ua} = $ua;

	$self->{transaction} = [];

	return $self;
}

sub connect(){
	my ($self, %param) = @_;
	$self->_needed(%param, required => "database");

	$self->{database} = $param{database};

	my $response = $self->_prepareRequest(
			method => "GET",
			uri	   => "connect/$param{database}"
		);
	
	if ($response->is_success) {
		 $self->{wrongAuth} = 0;
	}
	else {
		if($self->{wrongAuth}){
	 		die $response->status_line;
	 	}
	 	else{
	 		$self->{wrongAuth} = 1;
	 		$self->_auth();
	 		$self->_prepareRequest(
					method => "GET",
					uri	   => "connect/$param{database}"
	 			);
	 		$self->{ua}->default_header('Authorization' => "");
	 	}
	}	


	($self->{debug})&&(print "** Connected on $param{database}\n");
}

sub getDatabaseInfo(){
	my ($self, %param) = @_;

	my $database = $self->{database};
	if($param{database}){
		$database = $param{database};
	}

	my $response = $self->_prepareRequest(
			method => "GET",
			uri	   => "database/$database"
		);

	return JSON::decode_json ($response->{_content});
}

sub getClassInfo(){
	my ($self, %param) = @_;

	$self->_needed(%param, required => "class");

	my $database = $self->{database};
	if($param{database}){
		$database = $param{database};
	}

	my $response = $self->_prepareRequest(
			method => "GET",
			uri	   => "class/$database/$param{class}"
		);

	return JSON::decode_json ($response->{_content});
}

sub getCluster(){
	my ($self, %param) = @_;

	$self->_needed(%param, required => "name");

	my $database = $self->{database};
	if($param{database}){
		$database = $param{database};
	}

	my $response = $self->_prepareRequest(
			method => "GET",
			uri	   => "cluster/$database/$param{name}"
		);

	return JSON::decode_json ($response->{_content});
}

sub command(){
	my ($self, %param) = @_;

	$self->_needed(%param, required => "command");

	my $language = "sql";
	if($param{language}){
		$language = $param{language};
	}

	my $database = $self->{database};
	if($param{database}){
		$database = $param{database};
	}

	my $response = $self->_prepareRequest(
			method => "POST",
			uri	   => "command/$database/$language",
			data   => {
				'_content' => $param{command},
			}
		);

	return JSON::decode_json ($response->{_content});
}

sub beginTransaction(){
	my ($self, %param) = @_;	
	$self->{transaction} = [];
	return 1;
}

sub appendOperation(){
	my ($self, %param) = @_;	
	$self->_needed(%param, required => "type");

	if($param{type} eq "c" or $param{type} eq "CREATE"){
		$self->_createOperation(%param);
	}
	elsif($param{type} eq "d" or $param{type} eq "DELETE"){
		$self->_deleteOperation(%param);
	}
	elsif($param{type} eq "u" or $param{type} eq "UPDATE"){
		$self->_updateOperation(%param);
	}
	elsif($param{type} eq "script" or $param{type} eq "SCRIPT"){
		$self->_scriptOperation(%param);
	}
	elsif($param{type} eq "cmd" or $param{type} eq "CMD"){
		$self->_cmdOperation(%param);
	}
}

sub commit(){
	my ($self, %param) = @_;

	my $database = $self->{database};
	if($param{database}){
		$database = $param{database};
	}

	my $commit = {
			transaction => JSON::true,
			operations  => $self->{transaction}
		};
	
	my $json = JSON::encode_json($commit);

	my $response = $self->_prepareRequest(
			method => "POST",
			uri	   => "batch/$database/",
			data   => {
				'_content' => $json,
			}
		);

	$self->beginTransaction();

	if($response->is_success){
		return JSON::decode_json ($response->{_content});
	}
	else{
 		die $response->status_line;
	}
}

# =======================================================
# Private methods
# =======================================================

sub _needed(){
	my ($self, %param) = @_;
	foreach my $required (split(/\s+/, $param{required})){
		if(!exists $param{$required}){
			my @caller = caller;
			die("$caller[0]:$caller[2] Required $required!\n");
		}
	}
}

sub _createOperation(){
	my ($self, %param) = @_;	
	$self->_needed(%param, required => "class");

	my $operation = {
		type => "c",
		record => {}
	};

	foreach my $k (keys %param){
		my $value = $param{$k};

		if($self->_excludeFromRecord($k)){
			next;
		}
		if($self->_isSpecialField($k)){
			$k = '@'.$k;
		}

		$operation->{record}->{$k} = $value;
	}
	push(@{$self->{transaction}}, $operation);

}

sub _deleteOperation(){
	my ($self, %param) = @_;

	$self->_needed(%param, required => "rid");

	my $operation = {
		type => "d",
		record => {}
	};

	foreach my $k (keys %param){
		my $value = $param{$k};

		if($self->_excludeFromRecord($k)){
			next;
		}
		if($self->_isSpecialField($k)){
			$k = '@'.$k;
		}

		$operation->{record}->{$k} = $value;
	}
	push(@{$self->{transaction}}, $operation);		
}

sub _updateOperation(){
	my ($self, %param) = @_;	
	
	$self->_needed(%param, required => "rid");

	my $operation = {
		type => "u",
		record => {}
	};

	foreach my $k (keys %param){
		my $value = $param{$k};

		if($self->_excludeFromRecord($k)){
			next;
		}
		if($self->_isSpecialField($k)){
			$k = '@'.$k;
		}

		$operation->{record}->{$k} = $value;
	}
	push(@{$self->{transaction}}, $operation);	
}

sub _scriptOperation(){
	my ($self, %param) = @_;	

	$self->_needed(%param, required => "script");

	my $language = "sql";
	if(exists $param{language}){
		$language = $param{language};
	}	

	my $operation = {
		type => "script",
		language => $language,
		script => $param{script},
	};

	push(@{$self->{transaction}}, $operation);		

}

sub _cmdOperation(){
	my ($self, %param) = @_;	

	$self->_needed(%param, required => "command");

	my $language = "sql";
	if(exists $param{language}){
		$language = $param{language};
	}	

	my $operation = {
		type => "cmd",
		language => $language,
		command => $param{command},
	};

	push(@{$self->{transaction}}, $operation);	

}


sub _prepareRequest(){
	my ($self, %param) = @_;
	$self->_needed(%param, required => "method uri");

	my $method = "_$param{method}";
	
	my $response = $self->$method(%param);
	$self->{ua}->{cookie_jar}->extract_cookies( $response );

	return $response;
	
}

sub _GET(){
	my ($self, %param) = @_;

	my $query = "";
	if(exists $param{data}){
		foreach my $k (keys %{$param{data}}){
			$query .= "$k=$param{data}->{$k}&";
		}
	}

	my $response = $self->{ua}->get($self->{server} . "$param{uri}?$query");
	return $response;
}

sub _POST(){
	my ($self, %param) = @_;

	my $response = $self->{ua}->post($self->{server} . "$param{uri}", Content => $param{data}->{_content});
	return $response;
}

sub _auth(){
	my ($self, %param) = @_;

	($self->{debug})&&(print "** Prepare Basic Auth\n");
	
	use HTTP::Headers::ActionPack::Authorization::Basic;
	my $auth = HTTP::Headers::ActionPack::Authorization::Basic->new(
	  'Basic' => {
	      username => $self->{username},
	      password => $self->{password}
	  }
	);
	
	$self->{ua}->default_header('Authorization' => $auth);
}

# =======================================================
# Helper methods
# =======================================================

sub _excludeFromRecord(){
	my ($self, $k) = @_;
	if($k ~~ qw(type)){
		return 1;
	}
	return 0;
}

sub _isSpecialField(){
	my ($self, $k) = @_;

	my @special = ("class", "rid");

	if($k ~~ @special){
		return 1;
	}
	return 0;
}