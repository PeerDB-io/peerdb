# How to use:
# 1. Build the image: docker build -f mysql.Dockerfile --tag 'bin_mysql' .
# 2. Run the container: docker run --name alala -e MYSQL_ROOT_PASSWORD=<some password> -p 3306:3306 -d bin_mysql
FROM mysql:9.7.1@sha256:ae269281abffe401d65f04cb54d45a069a495b8174b9c0a815e502fed7fa0370

# Copy the sample configuration file into the container
COPY stacks/mysql/my.cnf.sample /etc/mysql/my.cnf

# Expose the default MySQL port
EXPOSE 3306

# Set the default command to run when starting the container
CMD ["mysqld"]
