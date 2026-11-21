#!/bin/sh
# Run one formal-methods job, the same way locally and in verify.yml: the job's own script, written by the owner of the
# spec, fuzz targets or proofs it checks. A job whose script doesn't exist yet passes with a notice, so the workflow
# can land before every spec does.
#
#   scripts/verify.sh tlc-ledger|tlc-worktree|tlc-relay|fuzz-smoke|kani
set -eu
root=${VERIFY_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}
job=${1:?usage: verify.sh JOB}
case $job in
    fuzz-smoke) set -- "$root/scripts/verify-fuzz.sh" 60 ;;  # 60 s per target: the smoke run
    *) set -- "$root/scripts/verify-$job.sh" ;;
esac
if [ ! -x "$1" ]; then
    echo "verify: no check yet for $job (${1#"$root"/} doesn't exist)"
    exit 0
fi
exec "$@"
