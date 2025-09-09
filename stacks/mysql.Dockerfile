# How to use:
# 1. Build the image: docker build -f mysql.Dockerfile --tag 'bin_mysql' .
# 2. Run the container: docker run --name alala -e MYSQL_ROOT_PASSWORD=<some password> -p 3306:3306 -d bin_mysql
FROM mysql:9.4.0@sha256:94254b456a6db9b56c83525a86bff4c7f1e52335f934cbed686fe1ce763116a0

# Copy the sample configuration file into the container
COPY stacks/mysql/my.cnf.sample /etc/mysql/my.cnf

# Expose the default MySQL port
EXPOSE 3306

# Set the default command to run when starting the container
CMD ["mysqld"]
