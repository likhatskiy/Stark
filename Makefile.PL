

use ExtUtils::MakeMaker;

WriteMakefile(
	NAME         => 'Stark',
	VERSION_FROM => 'lib/Stark.pm', # finds \$VERSION
	AUTHOR       => 'Alexey Likhatskiy <likhatskiy\@gmail.com>',
	ABSTRACT     => 'A perl sentry client for Mojolicious',
	PREREQ_PM    => {
		'Test::Simple'  => 0.44,
		'Mojolicious'   => 0,
	},
);
