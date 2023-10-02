#!perl

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use DBI;
use DBD::Pg ':async';
use Test::More;
use IO::Select;
require 'dbdpg_test_setup.pl';

my $debug = $ENV{DBDPG_DEBUG} || 0;
delete @ENV{ 'PGSERVICE', 'PGDATABASE' };

## Constants
isnt PG_CONNECTION_OK, undef, 'const: PG_CONNECTION_OK';
isnt PG_CONNECTION_BAD, undef, 'const: PG_CONNECTION_BAD';
isnt PG_CONNECTION_STARTED, undef, 'const: PG_CONNECTION_STARTED';
isnt PG_CONNECTION_MADE, undef, 'const: PG_CONNECTION_MADE';
isnt PG_CONNECTION_AWAITING_RESPONSE, undef, 'const: PG_CONNECTION_AWAITING_RESPONSE';
isnt PG_CONNECTION_AUTH_OK, undef, 'const: PG_CONNECTION_AUTH_OK';
isnt PG_CONNECTION_SETENV, undef, 'const: PG_CONNECTION_SETENV';
isnt PG_CONNECTION_SSL_STARTUP, undef, 'const: PG_CONNECTION_SSL_STARTUP';
isnt PG_CONNECTION_NEEDED, undef, 'const: PG_CONNECTION_NEEDED';
isnt PG_CONNECTION_CHECK_WRITABLE, undef, 'const: PG_CONNECTION_CHECK_WRITABLE';
isnt PG_CONNECTION_CONSUME, undef, 'const: PG_CONNECTION_CONSUME';
isnt PG_CONNECTION_GSS_STARTUP, undef, 'const: PG_CONNECTION_GSS_STARTUP';
isnt PG_CONNECTION_CHECK_TARGET, undef, 'const: PG_CONNECTION_CHECK_TARGET';
isnt PG_CONNECTION_CHECK_STANDBY, undef, 'const: PG_CONNECTION_CHECK_STANDBY';

isnt PG_POLLING_FAILED, undef, 'const: PG_POLLING_FAILED';
isnt PG_POLLING_READING, undef, 'const: PG_POLLING_READING';
isnt PG_POLLING_WRITING, undef, 'const: PG_POLLING_WRITING';
isnt PG_POLLING_OK, undef, 'const: PG_POLLING_OK';
isnt PG_POLLING_ACTIVE, undef, 'const: PG_POLLING_ACTIVE';

## We'll try various ways to get to a database to test with

## First, check to see if we've been here before and left directions
my ($testdsn,$testuser,$helpconnect,$su,$uid,$testdir,$pg_ctl,$initdb,$error,$version)
    = get_test_settings();

if ($debug) {
    Test::More::diag "Test settings:
dsn: $testdsn
user: $testuser
helpconnect: $helpconnect
su: $su
uid: $uid
testdir: $testdir
pg_ctl: $pg_ctl
initdb: $initdb
error: $error
version: $version
";
    for my $key ( grep { /^DBDPG/ } sort keys %ENV ) {
        Test::More::diag "ENV $key = $ENV{$key}\n";
    }
}

my $skip;
{
    my (undef, $connerror, $dbh) = connect_database();
    $skip = (! defined $dbh or $connerror);
    $dbh->disconnect if $dbh;
}

subtest 'successful async connect' => sub {
    if ($skip) {
        plan skip_all => 'Connection to database failed, cannot continue testing';
    }

    my $dbh = DBI->connect($testdsn, $testuser, $ENV{DBI_PASS}, {
        RaiseError => 0,
        PrintError => 0,
        pg_async_connect => 1,
    });

    isnt ($dbh, undef, 'got handle with pg_async_connect set to 1');
    is ($DBI::err, PG_CONNECTION_STARTED, 'operation in progress ($DBI::err)');
    is ($dbh->err, PG_CONNECTION_STARTED, 'operation in progress ($dbh->err)');
    is ($DBI::errstr, 'Operation now in progress', 'operation in progress ($DBI::errstr)');
    is ($dbh->errstr, 'Operation now in progress', 'operation in progress ($dbh->errstr)');

    my $state = PG_POLLING_WRITING;
    my ($rd, $wr) = (0, 0);
    while (PG_POLLING_OK != $state and PG_POLLING_FAILED != $state) {
        my $fd = $dbh->{pg_socket};

        my $sel = IO::Select->new($fd);
        if (PG_POLLING_WRITING == $state) {
            $sel->can_write;
            $wr++;
        } elsif (PG_POLLING_READING == $state) {
            $sel->can_read;
            $rd++;
        }
        $state = $dbh->pg_connection_poll;
    }
    is ($state, PG_POLLING_OK, 'connection established: $state');
    cmp_ok ($wr, '>', 1, "number of PG_POLLING_WRITING ($wr): >1");
    cmp_ok ($rd, '>', 0, "number of PG_POLLING_READING ($rd): >0");
    is ($DBI::err, undef, 'connection established ($DBI::err)');
    is ($DBI::errstr, undef, 'connection established ($DBI::errstr)');
    is ($dbh->err, undef, 'connection established ($dbh->err)');
    is ($dbh->errstr, undef, 'connection established ($dbh->errstr)');

    # run an async query
    my $rows_to_produce = 50000;
    my $sql = 'SELECT random() FROM generate_series(1,?)';
    my $sth = $dbh->prepare($sql, {pg_async => PG_ASYNC});
    $sth->execute($rows_to_produce);

    my $fd = $dbh->{pg_socket};
    my $sel = IO::Select->new($fd);
    my $loops = 0;
    $loops++ while (!$dbh->pg_ready and $sel->can_read);
    my $res = $sth->pg_result;
    my $rows_got=0;
    while ($sth->fetchrow_arrayref) {
        $rows_got++;
    }

    is ($res, $rows_to_produce, '->pg_result = '.$rows_to_produce);
    is ($rows_got, $rows_to_produce, '$rows_got = '.$rows_to_produce);
    cmp_ok ($loops, '>', 1, "number of while waiting for result ($loops): >1");

    done_testing;
};

subtest 'async connect immediate failure' => sub {
    my $alias = qr{(database|db|dbname)};
    if ($skip or $testdsn !~ /$alias\s*=\s*\S+/) {
        plan skip_all => 'Connection to database failed, cannot continue testing';
    }
    (my $dsn2 = $testdsn) =~ s/$alias\s*=/dbbarf=/;

    my $dbh = DBI->connect($dsn2, $testuser, $ENV{DBI_PASS}, {
        RaiseError => 0,
        PrintError => 0,
        pg_async_connect => 1,
    });

# according to 99_lint.t, all Test::More functions
# should start at the beginning of the line
    is ($dbh, undef, 'no handle');
    is ($DBI::err, PG_CONNECTION_BAD, 'PG_CONNECTION_BAD ($DBI::err)');
    is ($DBI::errstr, 'invalid connection option "dbbarf"', 'invalid option ($DBI::errstr)');

    done_testing;
};

subtest 'async connect delayed failure' => sub {
    if ($skip) {
        plan skip_all => 'Connection to database failed, cannot continue testing';
    }

    my $dbh = DBI->connect($testdsn, $testuser, 'invalid '.$ENV{DBI_PASS}, {
        RaiseError => 0,
        PrintError => 0,
        pg_async_connect => 1,
    });

    isnt ($dbh, undef, 'got handle with pg_async_connect set to 1');
    is ($DBI::err, PG_CONNECTION_STARTED, 'operation in progress ($DBI::err)');
    is ($DBI::errstr, 'Operation now in progress', 'operation in progress ($DBI::errstr)');
    is ($dbh->err, PG_CONNECTION_STARTED, 'operation in progress ($dbh->err)');
    is ($dbh->errstr, 'Operation now in progress', 'operation in progress ($dbh->errstr)');

    my $state = PG_POLLING_WRITING;
    my ($rd, $wr) = (0, 0);
    while (PG_POLLING_OK != $state and PG_POLLING_FAILED != $state) {
        my $fd = $dbh->{pg_socket};

        my $sel = IO::Select->new($fd);
        if (PG_POLLING_WRITING == $state) {
            $sel->can_write;
            $wr++;
        } elsif (PG_POLLING_READING == $state) {
            $sel->can_read;
            $rd++;
        }
        $state = $dbh->pg_connection_poll;
    }
    is ($state, PG_POLLING_FAILED, 'connection failed: $state');
    cmp_ok ($wr, '>', 1, "number of PG_POLLING_WRITING ($wr): >1");
    cmp_ok ($rd, '>', 0, "number of PG_POLLING_READING ($rd): >0");
    isnt ($DBI::err, undef, 'connection failed ($DBI::err)');
    isnt ($dbh->err, undef, 'connection failed ($dbh->err)');
    isnt ($DBI::errstr, undef, 'connection failed ($DBI::errstr)');
    isnt ($dbh->errstr, undef, 'connection failed ($dbh->errstr)');

    done_testing;
};

done_testing;
