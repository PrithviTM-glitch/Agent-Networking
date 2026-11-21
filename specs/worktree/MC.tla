---- MODULE MC ----
\* Model-checking wrapper: the bounds TLC needs. Run through scripts/verify-tlc-worktree.sh.
EXTENDS WorktreeLock
CONSTANTS main, lane

MCTrees == {main, lane}
\* time only matters relative to one lookup; 8 ticks cover every interleaving of two acquires and three lookups
MCBound == clock <= 8
====
