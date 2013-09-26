Files in this directory are sourced by /usr/sbin/titan after sourcing
titan.in.sh and titan-env.sh.  This allows storage backends, indexing
backends, other extensions, and local Titan modifications to customize
the Titan process's execution environment.

Only files ending in .sh.in will be sourced.  They are sourced in the
order returned by the shell-glob "*.sh.in".  In modern versions of
bash, "shell-glob order" is alphabetical according to $LC_COLLATE.

All files that don't end in .sh.in are ignored and are not sourced.
Files need not be executable to be sourced, only readable.
