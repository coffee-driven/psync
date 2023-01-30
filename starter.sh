#!/usr/bin/env sh

mkdir /run/sshd || exit 2

/usr/sbin/sshd -D
