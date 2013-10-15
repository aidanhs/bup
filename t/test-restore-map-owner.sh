#!/usr/bin/env bash
. ./wvtest-bup.sh

set -eo pipefail

if [ $(t/root-status) != root ]; then
    echo 'Not root: skipping restore --map-* tests.'
    exit 0 # FIXME: add WVSKIP.
fi

top="$(pwd)"
tmpdir="$(wvmktempdir)"
export BUP_DIR="$tmpdir/bup"

bup() { "$top/bup" "$@"; }

uid=$(id -u)
user=$(id -un)
gid=$(id -g)
group=$(id -gn)

other_uinfo=$(t/id-other-than --user "$user")
other_user="${other_uinfo%%:*}"
other_uid="${other_uinfo##*:}"

other_ginfo=$(t/id-other-than --group "$group")
other_group="${other_ginfo%%:*}"
other_gid="${other_ginfo##*:}"

bup init
cd "$tmpdir"

WVSTART "restore --map-user/group/uid/gid (control)"
mkdir src
touch src/foo
WVPASS bup index src
WVPASS bup save -n src src
WVPASS bup restore -C dest "src/latest/$(pwd)/src/"
WVPASS bup xstat dest/foo > foo-xstat
WVPASS grep -qE "^user: $user\$" foo-xstat
WVPASS grep -qE "^uid: $uid\$" foo-xstat
WVPASS grep -qE "^group: $group\$" foo-xstat
WVPASS grep -qE "^gid: $gid\$" foo-xstat

WVSTART "restore --map-user/group/uid/gid (user/group)"
rm -rf dest
# Have to remap uid/gid too because we're root and 0 would win).
WVPASS bup restore -C dest \
    --map-uid "$uid=$other_uid" --map-gid "$gid=$other_gid" \
    --map-user "$user=$other_user" --map-group "$group=$other_group" \
    "src/latest/$(pwd)/src/"
WVPASS bup xstat dest/foo > foo-xstat
WVPASS grep -qE "^user: $other_user\$" foo-xstat
WVPASS grep -qE "^uid: $other_uid\$" foo-xstat
WVPASS grep -qE "^group: $other_group\$" foo-xstat
WVPASS grep -qE "^gid: $other_gid\$" foo-xstat

WVSTART "restore --map-user/group/uid/gid (user/group trumps uid/gid)"
rm -rf dest
WVPASS bup restore -C dest \
    --map-uid "$uid=$other_uid" --map-gid "$gid=$other_gid" \
    "src/latest/$(pwd)/src/"
# Should be no changes.
WVPASS bup xstat dest/foo > foo-xstat
WVPASS grep -qE "^user: $user\$" foo-xstat
WVPASS grep -qE "^uid: $uid\$" foo-xstat
WVPASS grep -qE "^group: $group\$" foo-xstat
WVPASS grep -qE "^gid: $gid\$" foo-xstat

WVSTART "restore --map-user/group/uid/gid (uid/gid)"
rm -rf dest
bup restore -C dest \
    --map-user "$user=" --map-group "$group=" \
    --map-uid "$uid=$other_uid" --map-gid "$gid=$other_gid" \
    "src/latest/$(pwd)/src/"
WVPASS bup xstat dest/foo > foo-xstat
WVPASS grep -qE "^user: $other_user\$" foo-xstat
WVPASS grep -qE "^uid: $other_uid\$" foo-xstat
WVPASS grep -qE "^group: $other_group\$" foo-xstat
WVPASS grep -qE "^gid: $other_gid\$" foo-xstat

WVSTART "restore --map-user/group/uid/gid (zero uid/gid trumps all)"
rm -rf dest
bup restore -C dest \
    --map-user "$user=$other_user" --map-group "$group=$other_group" \
    --map-uid "$uid=0" --map-gid "$gid=0" \
    "src/latest/$(pwd)/src/"
WVPASS bup xstat dest/foo > foo-xstat
WVPASS grep -qE "^uid: 0\$" foo-xstat
WVPASS grep -qE "^gid: 0\$" foo-xstat

rm -rf "$tmpdir"
