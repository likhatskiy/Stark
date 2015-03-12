#!/usr/bin/env perl

use Test::More;
use lib qw(lib);
use Stark;

my $stark  = Stark->new();
$stark->custom_workers->{custom} = {};

my $worker = $stark->worker(
	worker_name => 'custom',
);
is $stark->stat->{idle_workers}, 1, 'right stat idle_workers';
is $worker->{worker_name}, 'custom', 'right worker name';
ok !$stark->enqueue('task' => 1), 'enqueue unknown job task';

$stark->add_task('task' => sub {});
ok !$stark->enqueue('task' => 1), 'right error';
ok !$stark->enqueue('task' => 1 => 'custom_bad'), 'right error ';
ok  $stark->enqueue('task' => 1 => 'custom'), 'right';

$stark->manage_jobs;

is $stark->stat->{idle_workers}, 0, 'right stat idle_workers';
is $stark->stat->{active_jobs }, 1, 'right stat active_jobs';
is $worker->{jobs_accepted}, 1, 'right jobs_accepted';

$worker->unregister;

done_testing();
