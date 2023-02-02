#!/usr/bin/env sh

mkdir /run/sshd || exit 2

# Launch in foreground and log to STDERR
/usr/sbin/sshd -D -e
