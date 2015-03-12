package Stark::Worker;
use Mojo::Base 'Stark::RPC';
use Storable qw(thaw nfreeze);
use Mojo::IOLoop;
use Data::Dumper;

has [qw(id stark pid ioloop worker_name)];
has jobs   => sub { {} };

sub new {
	my $self = shift->SUPER::new(@_);

	$self->id($self->_id);
	$self->{state        } = 'idle';
	$self->{jobs_accepted} = 0;
	$self->stark->workers->{$self->id} = $self;

	return $self;
}

sub log { shift->stark->log }
sub init {};

sub enqueue {
	my ($self, $job) = (shift, shift);
	unless (ref $job eq 'HASH' && $job->{id} && $job->{task}) {
		$self->log->error('Bad job. Cancel enqueue.');
		return;
	}

	$self->{state} = 'busy';
	++$self->{jobs_accepted};
	++$self->{jobs_active  };

	$job->{state} = 'active';
	$self->jobs->{$job->{id}} = $job;
	$self->send($job);
}

sub dequeue { delete shift->jobs->{ +shift } }

sub send {
	my ($self, $job) = (shift, shift);
	return unless $self->stark->{started};

	$self->log->debug("Job $job->{id} was sent to $self->{pid} worker.")
		if print { $self->stark->handler($self->pid) } nfreeze $job;
}

sub unregister {
	my $self = shift;

	$self->{running} = 0;

	return unless $self->stark->workers->{$self->{id}};

	delete $self->stark->workers->{$self->{id}};

	for (values %{ $self->jobs }) {
		$_->{state} = 'inactive';
	}

	if ($self->{pid}) {
		$self->stark->drop_handle($self->{pid});
		delete $self->stark->{workers_pid}->{$self->{pid}};

		$self->log->info("Worker [ID $self->{id}".($self->worker_name ? ', '.$self->worker_name : '')."] $self->{pid} stopped.");
	}

	$self->stark->emit(worker_finish => $self);
}

sub run {
	my $self = shift;
	
	my ($rpc_p, $rpc_c) = $self->portable_socketpair();
	
	$self->{time_started  } = time;
	$self->{running       } = 1;
	
	die "Can't fork: $!" unless defined(my $pid = fork);

	if ($pid) {
		# parent
		$self->pid($pid);

		$self->stark->{workers_pid}->{$pid} = $self;
		
		close $rpc_p;
		
		$self->stark->add_handler($rpc_c => $pid);
		$self->stark->watch($pid);

		$self->stark->emit(worker_spawn => $self);
	} else {
		# child

		# Clean worker environment
		$SIG{$_} = 'DEFAULT' for qw(INT CHLD TTIN TTOU);
		$SIG{TERM} = sub { _stop($self) };

		$self->pid($$);
		$self->change_name;

		$self->stark->ioloop(undef);
		$self->stark->jobs   ({});
		$self->stark->workers({});

		$self->ioloop(Mojo::IOLoop->new);

		close $rpc_c;
		
		$self->add_handler($rpc_p);
		$self->watch;

		if (my $init = $self->stark->{worker_init}) {
			eval { $init->($self) };
		} else {
			eval { $self->init() };
		}
		$self->log->error("[$$] Init failed: $@") if $@;

		$self->log->info("Worker [ID $self->{id}".($self->worker_name ? ', '.$self->worker_name : '')."] $$ started.");

		$self->ioloop->start;

		exit 0;
	}
	
	return $self;
}

sub change_name {
	my $self = shift;
	$0 = $self->stark->name.': worker'.($self->worker_name ? ' '.$self->worker_name : '').' ['.$self->{state}.']';
}

sub cleanup {
	my ($self, $force) = @_;
	
	return if !$force
		&& (
			$self->{running}
			|| defined $self->{hdr_stdout}
			|| defined $self->{hdr_stdres}
			|| defined $self->{hdr_stderr}
		);
	
	$self->unregister;
}

sub rpc_handler {
	my $self = shift;
	my $job  = eval { thaw shift };

	$self->log->error("[$$] Worker $self->{id} cant thaw job: $@") and return
		if $@;

	$self->log->debug("[$$] Worker received a job $job->{id}.");

	$self->{state} = 'busy';
	$self->change_name;

	my $result = $self->run_job($job);
	$self->dequeue($job->{id});
	
	$self->{state} = 'idle';
	$self->change_name;

	$self->log->debug("[$$] Exec job $job->{id} with '$job->{task}' task: ". $result->{state});
	if ($result->{error}) {
		$self->log->error("[$$] Exec job $job->{id} with '$job->{task}' failed: ". $result->{error});
	}

	print { $self->handler } nfreeze($result);
}

sub run_job {
	my $self = shift;
	my $job  = shift;

	my $task   = $self->stark->tasks->{$job->{task}};
	$self->log->error("[$$] Task '$job->{task}' of job $job->{id} not found.") and return
		unless $task;

	my $result = eval { $task->($self, $job->{args}) };
	return {
		id    => $job->{id},
		state => $@ ? 'failed' : 'finished',
		$@ ? (error => $@) : (result => $result),
	};
}

sub _stop {
	my $self = shift;
	$self->drop_handle;
	$self->ioloop->stop;
}

1;
