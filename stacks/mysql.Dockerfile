# How to use:
# 1. Build the image: docker build -f mysql.Dockerfile --tag 'bin_mysql' .
# 2. Run the container: docker run --name alala -e MYSQL_ROOT_PASSWORD=<some password> -p 3306:3306 -d bin_mysql
FROM mysql:9.7.0@sha256:c11782aa2a96624c1efc121768641d96954faa136d6aa82751b032d8c426ffbc

# Copy the sample configuration file into the container
COPY stacks/mysql/my.cnf.sample /etc/mysql/my.cnf

# Expose the default MySQL port
EXPOSE 3306

# Set the default command to run when starting the container
CMD ["mysqld"]
