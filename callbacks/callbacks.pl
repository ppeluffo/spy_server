#!/usr/bin/perl

use Sys::Syslog qw( :DEFAULT setlogsock);
setlogsock('unix');
openlog("SPY",'','user');

my $dlgid = $ARGV[0];

my $msg = '';
	

$msg = 'Testing callbacks with $dlgid';
syslog('info', $msg);
exit 0;
