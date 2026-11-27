#!/bin/sh

# Temporal's auto-setup creates the default namespace some time after the server
# starts, so retry until the MirrorName search attribute exists in it.
until temporal operator search-attribute list --namespace default 2>/dev/null | grep -qw MirrorName; do
    temporal operator search-attribute create --name MirrorName --type Text --namespace default >/dev/null 2>&1 || sleep 1
done

tini -s -- sleep infinity
