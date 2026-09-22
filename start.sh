#!/bin/sh
# Render-only startup script: runs the mock upstream in the background and
# the real gateway in the foreground, both in one container.
#
# This exists because render.yaml's dockerCommand is a plain string, and
# embedding this two-process "background + exec" logic directly in that
# YAML field (wrapped in its own nested sh -c "...") produced a garbled,
# mis-parsed command on Render's side - however Render tokenizes
# dockerCommand, it did not preserve the embedded quoting the way a local
# `sh -c "..."` test did. A plain script file with no inline shell
# metacharacters in the YAML sidesteps that entirely: `dockerCommand: sh
# start.sh` can't be mis-tokenized regardless of whether Render treats it
# as shell-form, splits it naively, or shlex-parses it - it's two words,
# no quotes, no `&`, nothing to mangle.
#
# Not used by docker-compose.yml locally - that setup keeps mock_upstream
# as its own container/process, which is the more accurate architecture.
# This script is a Render-deployment-only simplification (see render.yaml's
# top-of-file comment for why).

uvicorn mock_upstream:app --host 0.0.0.0 --port 9000 --loop uvloop &
exec gunicorn main:app --workers 2 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000 --timeout 30
