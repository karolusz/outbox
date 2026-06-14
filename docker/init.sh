#!/bin/sh
#
# Bootstrap script for the docker-compose test Postgres. The official
# postgres image runs anything in /docker-entrypoint-initdb.d/ on first DB
# init. This script applies the lib's *.up.sql migrations (in lexical
# order, which equals chronological order because filenames are
# timestamp-prefixed).
#
# This is the simplest viable bootstrap. When the lib ships its own
# migration tool, this script gets replaced by a call to that tool.

set -e

for f in /migrations/*.up.sql; do
    [ -f "$f" ] || continue
    echo "init.sh: applying $f"
    psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --file "$f"
done
