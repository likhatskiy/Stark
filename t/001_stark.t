#!/usr/bin/env perl

use Test::More;
use lib qw(lib);
use Stark;

my $stark  = Stark->new;

my $worker1 = $stark->worker;
is $stark->stat->{idle_workers}, 1, 'right stat idle_workers';
$worker1->unregister;

ok !$stark->workers->{$worker1->{id}}, 'right unregister';
is $stark->stat->{idle_workers}, 0, 'right stat idle_workers';

my $worker1 = $stark->worker;
my $worker2 = $stark->worker;

ok $worker1, 'add worker';
is $stark->workers->{$worker1->id}->id, $worker1->id, 'check worker';
is $worker1->{state}, 'idle', 'right state';
is $worker1->{jobs_accepted}, 0, 'right jobs_accepted';
ok $worker2, 'add worker';
isnt $worker1->id, $worker2->id, 'new worker id';

my $stat = $stark->stat;
is $stat->{idle_workers  }, 2, 'right stat idle_workers';
is $stat->{busy_workers  }, 0, 'right stat busy_workers';
is $stat->{active_jobs   }, 0, 'right stat active_jobs';
is $stat->{inactive_jobs }, 0, 'right stat inactive_jobs';
is $stat->{failed_jobs   }, 0, 'right stat failed_jobs';
is $stat->{finished_jobs }, 0, 'right stat finished_jobs';

ok !$stark->enqueue('task_name' => 1), 'enqueue unknown job task';
ok !$stark->dequeue(123), 'dequeue unknown job task';

$stark->add_task('task_name' => sub { return [12/$_[1], 5] });
ok $stark->tasks->{'task_name'}, 'add task';

my $job = $stark->enqueue('task_name' => 1);
is ref $job, HASH, 'add job';
ok $stark->jobs->{$job->{id}}, 'right job';
is $job->{task }, 'task_name', 'right job';
is $job->{state}, 'inactive', 'right job';
is $job->{args }, 1, 'right job';
is $stark->stat->{inactive_jobs}, 1, 'right stat active_jobs';
is (($stark->dequeue($job->{id}) || {})->{id}, $job->{id}, 'dequeue');
is $stark->stat->{inactive_jobs}, 0, 'right stat active_jobs';

my $job    = $stark->enqueue('task_name' => 3);
my $result = $worker1->run_job($job);
ok $result, 'right run job';
is $result->{state }, 'finished', 'right state run job';
is $result->{result}->[0], 4, 'right args run job';
is $result->{result}->[1], 5, 'right result run job';

my $job    = $stark->enqueue('task_name' => 0);
my $result = $worker1->run_job($job);
ok $result, 'right run job';
is  $result->{state }, 'failed', 'right state run job';
ok !$result->{result}, 'right args run job';
like  $result->{error}, qr/Illegal division by zero/, 'right error run job';

done_testing();
