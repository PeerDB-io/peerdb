#!/bin/sh
# Usage: extract.sh <mysql-src> <maria-src>  (writes internal/dict/{mysql,mariadb}.txt)
set -eu
mysql_src=$1 maria_src=$2 out=$(dirname "$0")
awk '/SYM\("/{ if (match($0,/"[^"]+"/)) print substr($0,RSTART+1,RLENGTH-2) }' \
  "$mysql_src/sql/lex.h" | LC_ALL=C sort -u > "$out/mysql.txt"
awk '/^[[:space:]]*\{[[:space:]]*"/{ if (match($0,/"[^"]+"/)) print substr($0,RSTART+1,RLENGTH-2) }' \
  "$maria_src/sql/lex.h" | LC_ALL=C sort -u > "$out/mariadb.txt"
