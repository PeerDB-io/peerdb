# How to use:
# 1. Build the image: docker build -f mysql.Dockerfile --tag 'bin_mysql' .
# 2. Run the container: docker run --name alala -e MYSQL_ROOT_PASSWORD=<some password> -p 3306:3306 -d bin_mysql
FROM mysql:9.7.1@sha256:cbade841779f1661300e705721f9d2ff159865cc7a8a291affbff43ac6ec7f1d

# Copy the sample configuration file into the container
COPY stacks/mysql/my.cnf.sample /etc/mysql/my.cnf

# Expose the default MySQL port
EXPOSE 3306

# Set the default command to run when starting the container
CMD ["mysqld"]
