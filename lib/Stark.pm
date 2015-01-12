package Stark;
use Mojo::Base 'Stark::RPC';
use Mojo::IOLoop;
use Mojo::Log;
use POSIX ":sys_wait_h";
use Storable qw(thaw nfreeze);
use Data::Dumper;

has name             => sub { 'stark' };
has worker_class     => sub { 'Stark::Worker' };
has accept_interval  => sub { 0.25 };
has graceful_timeout => sub { 10   };
has custom_workers   => sub { {} };
has worker_count     => sub { 1 };
has task_limit       => sub { 0 };
has tasks            => sub { {} };
has jobs             => sub { {} };
has workers          => sub { {} };
has log              => sub { Mojo::Log->new          };
has ioloop           => sub { Mojo::IOLoop->singleton };

our $VERSION = 0.1;

sub recurring { shift->ioloop->recurring(@_) }

sub add_task {
	my ($self, $name, $cb) = @_;
	$self->tasks->{$name} = $cb;
	return $self;
}

sub enqueue {
	my ($self, $task) = (shift, shift);
	my $args          = shift;

	return unless $self->tasks->{$task};

	my $job = {
		id       => $self->_id,
		args     => $args,
		created  => time,
		state    => 'inactive',
		task     => $task,
	};

	$self->jobs->{$job->{id}} = $job;
	return $job;
}

sub dequeue { delete shift->jobs->{ +shift } }

sub run {
	my $self = shift;

	$self->log->info("Stark $$ started.");
	$0 = $self->name.': master';

	$SIG{CHLD} = sub { _sig_chld($self) };
	$SIG{TERM} = sub { _stop    ($self) };

	$self->custom_workers();
	$self->worker_count();

	$self->manage;

	$self->ioloop->recurring($self->accept_interval => sub {
		$self->manage;
		return if $self->{finished};

		my @jobs = grep {$_->{state} eq 'inactive'} values %{ $self->{jobs} };
		return unless @jobs;

		my @workers = 
			grep { $_->{state} eq 'idle' }
			sort {$a->{jobs_accepted} <=> $b->{jobs_accepted}}
			values %{ $self->{workers} }
		;

		for (@workers) {
			$_->enqueue(shift @jobs);
			last unless @jobs;
		}
	});

	$self->ioloop->start;
	exit 0;
}

sub manage {
	my $self = shift;

	if (!$self->{finished}) {
		unless (%{ $self->{custom_workers} }) {
			$self->worker->run while keys %{$self->{workers}} < $self->{worker_count};
		} else {
			for my $name (keys %{ $self->{custom_workers} }) {
				$self->worker(
					worker_name => $name,
					%{ $self->{custom_workers}->{$name} },
				)->run unless grep {$_->{worker_name} eq $name} values %{ $self->workers };
			}
		}
	# Shutdown
	} elsif (!keys %{ $self->{workers} }) {
		$self->log->info("Stark stopped.");
		$self->ioloop->stop;
		return;
	}

	for my $worker(values %{ $self->workers }) {
		next unless $worker->{graceful};

		if (!$worker->{force} && time > $worker->{graceful} + $self->graceful_timeout) {
			$self->log->debug("Stopping worker $worker->{pid} by graceful_timeout.");
			
			$worker->{force} = 1;
			kill 'KILL', $worker->pid;
		}
	}
}

sub worker {
	my $self = shift;
	my $p    = @_ > 1 ? {@_}  : undef;
	my $pid  = $p     ? undef : shift;

	return $self->{workers_pid}->{$pid} if $pid;

	my $worker_class = ($p || {})->{class} || $self->worker_class;

	unless ($worker_class->can('new')) {
		(my $file = $worker_class) =~ s{::|'}{/}g;
		require "$file.pm";
	}

	my $worker = $worker_class->new(
		stark => $self,
		%{ $p || {} },
	);
	$self->emit(worker_add => $worker);

	return $worker;
}

sub stat {
	my $self = shift;

	my @jobs    = values %{$self->jobs   };
	my @workers = values %{$self->workers};

	return {
		idle_workers   => scalar(grep { $_->{state} eq 'idle'     } @workers),
		busy_workers   => scalar(grep { $_->{state} eq 'busy'     } @workers),
		active_jobs    => scalar(grep { $_->{state} eq 'active'   } @jobs   ),
		inactive_jobs  => scalar(grep { $_->{state} eq 'inactive' } @jobs   ),
		failed_jobs    => scalar(grep { $_->{state} eq 'failed'   } @jobs   ),
		finished_jobs  => scalar(grep { $_->{state} eq 'finished' } @jobs   ),
	};
}

sub rpc_handler {
	my $self       = shift;
	my $result     = eval {thaw shift};
	my $worker_pid = shift;

	$self->log->error("Stark cant thaw task result: $@") and return
		if $@;

	$self->log->debug("Job $result->{id} is $result->{state} on $worker_pid worker.");

	if (my $worker = $self->worker($worker_pid)) {
		if ($self->task_limit > 0 && $worker->{jobs_accepted} >= $self->task_limit) {
			$worker->{state} = 'stop';
			$self->term_worker($worker);
		} else {
			$worker->{state} = 'idle';
			--$worker->{jobs_active};
		}
	}

	$self->emit('finish_job' => $result);
	
	if ($result->{state} eq 'finished') {
		$self->dequeue($result->{id});
		return;
	}

	($self->jobs->{$result->{id}} || {})->{state} = $result->{state};
}

sub _sig_chld {
	my $self = shift;

	while ((my $pid = waitpid(-1, WNOHANG)) > 0) {
		if (my $worker = $self->worker($pid)) {
			$worker->unregister;
		}
	}
}

sub _stop {
	my $self = shift;
	$self->{finished} = 1;

	for my $worker(values %{ $self->workers }) {
		$worker->term_worker($worker);
	}
}

sub term_worker {
	my $self   = shift;
	my $worker = shift;

	$self->log->debug("Trying to stop worker $worker->{pid} gracefully.");
	$worker->{graceful} = time;

	kill 'TERM', $worker->pid;
}

1;