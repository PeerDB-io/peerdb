#!/bin/sh
printenv > /app/.env.production
exec "$@"
