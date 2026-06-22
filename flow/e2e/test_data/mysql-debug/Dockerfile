# MySQL debug build for e2e binlog INCIDENT_EVENT testing.
#
# The official mysql image ships only the minimal server, which is not compiled WITH_DEBUG and
# therefore lacks the DBUG facility needed to inject a LOST_EVENTS incident at runtime
# (SET GLOBAL debug='+d,binlog_inject_incident'). We swap the minimal server for the full server
# plus mysqld-debug, then route only the final foreground server to mysqld-debug. The stock
# entrypoint's config checks, datadir initialization, and temporary init server keep using the
# release binary because the debug binary can trip InnoDB assertions on that path.
ARG MYSQL_VERSION=8.0.46
FROM mysql:${MYSQL_VERSION}
ARG MYSQL_VERSION

LABEL org.opencontainers.image.source="https://github.com/PeerDB-io/peerdb" \
      org.opencontainers.image.title="PeerDB MySQL Debug" \
      org.opencontainers.image.description="MySQL ${MYSQL_VERSION} debug image for PeerDB e2e tests"

# Keep the debug/full-server RPMs exactly matched to the server-minimal RPM in the base image.
# This lets MYSQL_VERSION move without independently hard-coding package versions here.
#
# Installing mysql-community-server rewrites /etc/my.cnf with RPM defaults, so preserve the
# official Docker image config first. It keeps server logs on stderr and includes conf.d.
RUN set -eux; \
    mysql_rpm_version="$(rpm -q --qf '%{VERSION}-%{RELEASE}.%{ARCH}' mysql-community-server-minimal)"; \
    cp /etc/my.cnf /tmp/docker-my.cnf; \
    printf '%s\n' \
      '[mysql80-community]' \
      'name=MySQL 8.0 Community Server' \
      'baseurl=https://repo.mysql.com/yum/mysql-8.0-community/el/9/$basearch/' \
      'enabled=1' \
      'gpgcheck=1' \
      'gpgkey=https://repo.mysql.com/RPM-GPG-KEY-mysql-2023' \
      > /etc/yum.repos.d/mysql-community-debug.repo \
 && microdnf -y remove mysql-community-server-minimal \
 && microdnf -y --setopt=install_weak_deps=0 install \
      "mysql-community-client-${mysql_rpm_version}" \
      "mysql-community-client-plugins-${mysql_rpm_version}" \
      "mysql-community-common-${mysql_rpm_version}" \
      "mysql-community-icu-data-files-${mysql_rpm_version}" \
      "mysql-community-libs-${mysql_rpm_version}" \
      "mysql-community-server-${mysql_rpm_version}" \
      "mysql-community-server-debug-${mysql_rpm_version}" \
 && microdnf clean all \
 && rm -f /etc/yum.repos.d/mysql-community-debug.repo

# Restore the official Docker config, but keep the RPM socket path. The official image normally
# uses /var/run/mysqld/mysqld.sock and creates a compatibility symlink in /var/lib/mysql; mysqld-debug
# asserts while scanning that dangling symlink during final startup.
RUN set -eux; \
    cp /tmp/docker-my.cnf /etc/my.cnf; \
    sed -i 's!socket=/var/run/mysqld/mysqld.sock!socket=/var/lib/mysql/mysql.sock!g' /etc/my.cnf; \
    rm -f /tmp/docker-my.cnf

# Route entrypoint checks, datadir init, and the temporary init server to the release binary.
# Only the final foreground server should use mysqld-debug; running debug for init/temp startup
# trips InnoDB debug assertions before the test can run.
RUN set -eux; \
    mv /usr/sbin/mysqld /usr/sbin/mysqld-release; \
    printf '%s\n' \
      '#!/bin/bash' \
      'for arg in "$@"; do' \
      '  case "$arg" in' \
      '    --initialize|--initialize-insecure|--daemonize|--verbose|--help|--validate-config)' \
      '      exec /usr/sbin/mysqld-release "$@"' \
      '      ;;' \
      '  esac' \
      'done' \
      'exec /usr/sbin/mysqld-debug "$@"' \
      > /usr/sbin/mysqld; \
    chmod +x /usr/sbin/mysqld
