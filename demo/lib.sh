# Shared helpers for the Rabbit Pricing Optimizer demo recordings.
# Source this from a chapter script; do not run it directly.
#
# SECRET SAFETY: always single-quote the argument to run()/runq() so that
# things like $RABBIT_API_KEY are printed as the literal variable name and only
# expanded at execution time. Never pass a double-quoted string containing a
# secret variable to these functions.

_dim=$'\033[2m'; _bold=$'\033[1m'; _reset=$'\033[0m'

# say "..."  -> a narration cue for you only. Not printed, not executed.
say() { :; }

# note "..." -> a dim on-screen comment. Safe to show on camera.
note() { printf '%s# %s%s\n' "$_dim" "$*" "$_reset"; }

# pause      -> wait for Enter. Your beat / cut point between steps.
pause() { printf '%s'"$_dim"'--- (Enter) ---'"$_reset"'%s' '' ''; IFS= read -r _ || true; printf '\n'; }

# run 'cmd'  -> show the command (as if typed), wait for Enter, then execute it.
run() {
  printf '%sdemo$%s %s' "$_bold" "$_reset" "$*"
  IFS= read -r _ || true
  eval "$*"
  local rc=$?
  return $rc
}

# runq 'cmd' -> like run() but no pre-execution pause (fast successive commands).
runq() {
  printf '%sdemo$%s %s\n' "$_bold" "$_reset" "$*"
  eval "$*"
}

# clear_screen -> clear + wipe scrollback so no earlier secret is scrollable on camera.
clear_screen() { clear; printf '\033[3J'; }

# guard_env VAR... -> abort if any required var is unset or empty.
guard_env() {
  local missing=0 v
  for v in "$@"; do
    if [ -z "${!v:-}" ]; then printf 'MISSING env var: %s\n' "$v" >&2; missing=1; fi
  done
  if [ "$missing" -ne 0 ]; then
    printf '\nFill these in demo/00_env.sh (copy from demo/00_env.example.sh), then:\n' >&2
    printf '  source demo/00_env.sh\n' >&2
    exit 1
  fi
}
