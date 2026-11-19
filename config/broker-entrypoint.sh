#!/bin/sh
# The broker's entrypoint under compose.auth.yaml (docs/AUTH.md 4.1, 4.2 and 4.8). It renders mosquitto's config from
# config/mosquitto.conf and AGENT_BRIDGE_AUTH:
#   off      today's broker, plus the unix socket when its directory is mounted;
#   warn     the dynamic-security plugin; clients with credentials get their roles, anonymous ones the legacy group
#            with full rights, so 0.3.1 clients keep working;
#   enforce  as warn, but anonymous clients are refused.
# It runs mosquitto as the owner of the mounted directories (the host user), so the socket is theirs, and at the first
# warn or enforce start it writes the admin and service credentials into the mounted config dir.
set -eu

mode="${AGENT_BRIDGE_AUTH:-off}"
case "$mode" in
off | warn | enforce) ;;
*)
	echo "AGENT_BRIDGE_AUTH must be off, warn or enforce, got '$mode'" >&2
	exit 1
	;;
esac

base=/mosquitto/config/mosquitto.conf
conf=/tmp/agent-bridge-mosquitto.conf
run=/run/agent-bridge
cfg=/agent-bridge-config
dynsec=/mosquitto/data/dynamic-security.json
ready=/tmp/agent-bridge-ready
rm -f "$ready"

# the host user: the owner of the mounted config dir, else of the socket dir, else the image's mosquitto user
uid=1883 gid=1883
for d in "$cfg" "$run"; do
	if [ -d "$d" ]; then
		uid=$(stat -c %u "$d") gid=$(stat -c %g "$d")
		break
	fi
done
if [ "$uid" = 0 ]; then
	echo "refusing to run the broker as root: the mounted directory belongs to root (Docker creates a missing bind source as root); remove it and create it as yourself" >&2
	exit 1
fi
if [ "$mode" != off ] && [ ! -d "$cfg" ]; then
	echo "AGENT_BRIDGE_AUTH=$mode needs the config dir mounted at $cfg (compose.auth.yaml)" >&2
	exit 1
fi
chown -R "$uid:$gid" /mosquitto/data

grep -v '^allow_anonymous' "$base" >"$conf"
if [ -d "$run" ]; then
	rm -f "$run/mqtt.sock"
	echo "listener 0 $run/mqtt.sock" >>"$conf"
fi
if [ "$mode" = off ]; then
	echo "allow_anonymous true" >>"$conf"
else
	{
		echo "plugin /usr/lib/mosquitto_dynamic_security.so"
		echo "plugin_opt_config_file $dynsec"
		echo "check_retain_source true"
		if [ "$mode" = warn ]; then echo "allow_anonymous true"; else echo "allow_anonymous false"; fi
	} >>"$conf"
fi

# busybox has no setpriv --reuid: the image's mosquitto user takes the host user's ids instead
sed -i "s/^mosquitto:x:[0-9]*:[0-9]*:/mosquitto:x:$uid:$gid:/" /etc/passwd
sed -i "s/^mosquitto:x:[0-9]*:/mosquitto:x:$gid:/" /etc/group
secret() { head -c 24 /dev/urandom | base64 | tr -d '/+=\n'; }
field() { sed -n "s/.*\"$1\": *\"\([^\"]*\)\".*/\1/p" "$2"; }

# credentials files: JSON {username, password, client_id}, 0600 in 0700 directories, owned by the host user
write_creds() {
	umask 077
	printf '{"username": "%s", "password": "%s", "client_id": "%s"}\n' "$2" "$3" "$(secret)" >"$1.tmp"
	chown "$uid:$gid" "$1.tmp"
	mv "$1.tmp" "$1"
}

if [ "$mode" != off ]; then
	install -d -m 700 -o "$uid" -g "$gid" "$cfg/services"
	if [ ! -f "$dynsec" ]; then
		pw=$(secret)
		mosquitto_ctrl dynsec init "$dynsec" admin "$pw" >/dev/null
		chown "$uid:$gid" "$dynsec"
		write_creds "$cfg/admin.json" admin "$pw"
	fi
	if [ ! -f "$cfg/admin.json" ]; then
		echo "$cfg/admin.json is missing but the broker's data volume has its dynamic-security store; restore the file, or start over with: docker compose run --rm --entrypoint rm mosquitto $dynsec" >&2
		exit 1
	fi
	printf '%s\n' "$mode" >"$cfg/auth-mode.tmp"
	chown "$uid:$gid" "$cfg/auth-mode.tmp"
	mv "$cfg/auth-mode.tmp" "$cfg/auth-mode"
else
	rm -f "$cfg/auth-mode" 2>/dev/null || true
fi

# one command string (busybox su runs arguments after -c CMD as a script), and no function or subshell around it, so
# $pid is mosquitto itself: su and its shell exec into it
su -s /bin/sh mosquitto -c "exec mosquitto -c $conf" &
pid=$!
trap 'kill -TERM "$pid" 2>/dev/null' TERM INT
trap 'kill -HUP "$pid" 2>/dev/null' HUP

if [ "$mode" != off ]; then
	admin_pw=$(field password "$cfg/admin.json")
	ctl() { mosquitto_ctrl -h 127.0.0.1 -p 1883 -u admin -P "$admin_pw" dynsec "$@" >/dev/null 2>&1; }
	tries=0
	until ctl getDefaultACLAccess; do
		tries=$((tries + 1))
		if [ "$tries" -ge 50 ] || ! kill -0 "$pid" 2>/dev/null; then
			echo "the broker did not answer as admin: is $cfg/admin.json the one for this data volume?" >&2
			kill -TERM "$pid" 2>/dev/null || true
			exit 1
		fi
		sleep 0.2
	done
	acl() { ctl addRoleACL "$1" "$2" "$3" "$4" "${5:-0}" || true; }
	for t in publishClientSend publishClientReceive subscribe; do ctl setDefaultACLAccess "$t" deny; done
	ctl setDefaultACLAccess unsubscribe allow
	# admin (the console): everything, besides the dynsec control topics init gave it
	for t in publishClientSend publishClientReceive subscribePattern; do acl admin "$t" '#' allow; done
	# anonymous clients in warn: full rights, as before auth
	ctl createRole legacy-all || true
	for t in publishClientSend publishClientReceive subscribePattern; do acl legacy-all "$t" '#' allow; done
	ctl createGroup legacy || true
	ctl addGroupRole legacy legacy-all || true
	ctl setAnonymousGroup legacy || true
	for svc in ledgerd historyd; do
		file="$cfg/services/$svc.json"
		[ -f "$file" ] || write_creds "$file" "$svc" "$(secret)"
		pw=$(field password "$file")
		ctl createClient "$svc" -p "$pw" || ctl setClientPassword "$svc" "$pw" || true
		ctl createRole "role-$svc" || true
		ctl addClientRole "$svc" "role-$svc" || true
	done
	for t in ledger/req ledger/req/+ agents/+ rooms/+ ledger/resp/ledgerd/+; do
		acl role-ledgerd subscribePattern "$t" allow
		acl role-ledgerd publishClientReceive "$t" allow
	done
	for t in 'ledger/resp/#' inbox/+ inbox/+/ledgerd ledger/req/ledgerd; do acl role-ledgerd publishClientSend "$t" allow; done
	acl role-historyd subscribePattern '#' allow
	acl role-historyd publishClientReceive '#' allow
	for t in 'history/resp/#' history/req/historyd; do acl role-historyd publishClientSend "$t" allow; done
fi

touch "$ready"
while kill -0 "$pid" 2>/dev/null; do
	wait "$pid" && status=0 || status=$?
done
exit "${status:-0}"
