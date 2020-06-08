#!/usr/bin/env perl
if (!$::Driver) { use FindBin; exec("$FindBin::Bin/bootstrap.pl", @ARGV, $0); die; }
# DESCRIPTION: Verilator: Verilog Test driver/expect definition
#
# Copyright 2019 by Wilson Snyder. This program is free software; you
# can redistribute it and/or modify it under the terms of either the GNU
# Lesser General Public License Version 3 or the Perl Artistic License
# Version 2.0.
# SPDX-License-Identifier: LGPL-3.0-only OR Artistic-2.0

scenarios(simulator => 1);

# FIXME how do we do timing advance in SystemC?

compile(
    #FIXME
    #verilator_flags2 => ['--exe --build --main --timing'],
    verilator_flags2 => ['--exe --build --main --timing -CFLAGS -DVL_DEBUG'],
    verilator_make_cmake => 0,
    verilator_make_gmake => 0,
    make_main => 0,
    make_top => 1,
    );

execute(
    check_finished => 1,
    #FIXME
    all_run_flags => ['+verilator+debugi+9'],
    );

ok(1);
1;