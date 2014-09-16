package Stark::RPC;
use Mojo::Base 'Mojo::EventEmitter';
use bytes;
use Errno;
use Socket;
use Time::HiRes qw(gettimeofday tv_interval sleep);
use Mojo::Util qw(md5_sum);

use constant MONOTONIC => eval
  '!!Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC())';

sub _id {
	my $self = shift;
	return md5_sum join '', $$, steady_time(), rand 100;
}

sub steady_time () {
	MONOTONIC
		? Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC())
		: Time::HiRes::time;
}

sub handler {
	my $self = shift;
	my $pid  = shift || $$;

	return $self->{handler}->{$pid};
}

sub add_handler {
	my $self = shift;
	my $hdr  = shift;
	my $pid  = shift || $$;

	$self->{handler}->{$pid} = $hdr;
}

sub drop_handle {
	my $self = shift;
	my $pid  = shift || $$;

	my $hdr = delete $self->{handler}->{$pid};
	return unless $hdr;
	
	$self->ioloop->remove( $hdr );
	undef $hdr;
	
	$self->log->debug("[$self->{pid}]: RPC closed.") if $ENV{STARK_IO_DEBUG};
	
	$self->cleanup;
}

sub watch {
	my $self = shift;
	my $pid  = shift || $$;

	my $hdr = $self->handler($pid);
	$self->log->error("[$self->{pid}]: RPC handler is EMPTY") and return unless $hdr;
	
	my $id = fileno $hdr;
	
	$self->ioloop->reactor->io($hdr, sub {
		my $chunk = undef;
		my $len   = sysread $hdr, $chunk, 65536;
		
		return unless defined $len or $! != Errno::EINTR;
		
		if (!$len) {
			$self->drop_handle($pid);
			return;
		}
		
		# $self->log->debug("[$$][rpc] $chunk");
		$self->emit('rpc', $pid => $chunk);
		$self->rpc_handler($chunk => $pid);

	# });
	})->watch($hdr, 1, 0);
}

sub cleanup { return 1 }

sub portable_pipe {
	my ($r, $w);
	pipe $r, $w or return;
	
	($r, $w);
}

sub portable_socketpair {
	socketpair my $fh1, my $fh2, Socket::AF_UNIX(), Socket::SOCK_STREAM(), PF_UNSPEC
		or return;
	$fh1->autoflush(1);
	$fh2->autoflush(1);
	
	($fh1, $fh2)
}

1;