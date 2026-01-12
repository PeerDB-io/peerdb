# How to use:
# 1. Build the image: docker build -f mysql.Dockerfile --tag 'bin_mysql' .
# 2. Run the container: docker run --name alala -e MYSQL_ROOT_PASSWORD=<some password> -p 3306:3306 -d bin_mysql
FROM mysql:9.5.0@sha256:c9f0c66a87356518d4b30dbc065eb4567e4a04aff4d0ff194dea0973e5985b57

# Copy the sample configuration file into the container
COPY stacks/mysql/my.cnf.sample /etc/mysql/my.cnf

# Expose the default MySQL port
EXPOSE 3306

# Set the default command to run when starting the container
CMD ["mysqld"]
