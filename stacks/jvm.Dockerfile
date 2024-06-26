####
# This Dockerfile is used in order to build a container that runs the Quarkus application in JVM mode
#
# If you want to include the debug port into your docker image
# you will have to expose the debug port (default 5005 being the default) like this :  EXPOSE 8080 5005.
# Additionally you will have to set -e JAVA_DEBUG=true and -e JAVA_DEBUG_PORT=*:5005
# when running the container
#
# Then run the container using :
#
# docker run -i --rm -p 8080:8080 quarkus/flow-jvm-jvm
#
# This image uses the `run-java.sh` script to run the application.
# This scripts computes the command line to execute your Java application, and
# includes memory/GC tuning.
# You can configure the behavior using the following environment properties:
# - JAVA_OPTS: JVM options passed to the `java` command (example: "-verbose:class")
# - JAVA_OPTS_APPEND: User specified Java options to be appended to generated options
#   in JAVA_OPTS (example: "-Dsome.property=foo")
# - JAVA_MAX_MEM_RATIO: Is used when no `-Xmx` option is given in JAVA_OPTS. This is
#   used to calculate a default maximal heap memory based on a containers restriction.
#   If used in a container without any memory constraints for the container then this
#   option has no effect. If there is a memory constraint then `-Xmx` is set to a ratio
#   of the container available memory as set here. The default is `50` which means 50%
#   of the available memory is used as an upper boundary. You can skip this mechanism by
#   setting this value to `0` in which case no `-Xmx` option is added.
# - JAVA_INITIAL_MEM_RATIO: Is used when no `-Xms` option is given in JAVA_OPTS. This
#   is used to calculate a default initial heap memory based on the maximum heap memory.
#   If used in a container without any memory constraints for the container then this
#   option has no effect. If there is a memory constraint then `-Xms` is set to a ratio
#   of the `-Xmx` memory as set here. The default is `25` which means 25% of the `-Xmx`
#   is used as the initial heap size. You can skip this mechanism by setting this value
#   to `0` in which case no `-Xms` option is added (example: "25")
# - JAVA_MAX_INITIAL_MEM: Is used when no `-Xms` option is given in JAVA_OPTS.
#   This is used to calculate the maximum value of the initial heap memory. If used in
#   a container without any memory constraints for the container then this option has
#   no effect. If there is a memory constraint then `-Xms` is limited to the value set
#   here. The default is 4096MB which means the calculated value of `-Xms` never will
#   be greater than 4096MB. The value of this variable is expressed in MB (example: "4096")
# - JAVA_DIAGNOSTICS: Set this to get some diagnostics information to standard output
#   when things are happening. This option, if set to true, will set
#  `-XX:+UnlockDiagnosticVMOptions`. Disabled by default (example: "true").
# - JAVA_DEBUG: If set remote debugging will be switched on. Disabled by default (example:
#    true").
# - JAVA_DEBUG_PORT: Port used for remote debugging. Defaults to 5005 (example: "8787").
# - CONTAINER_CORE_LIMIT: A calculated core limit as described in
#   https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt. (example: "2")
# - CONTAINER_MAX_MEMORY: Memory limit given to the container (example: "1024").
# - GC_MIN_HEAP_FREE_RATIO: Minimum percentage of heap free after GC to avoid expansion.
#   (example: "20")
# - GC_MAX_HEAP_FREE_RATIO: Maximum percentage of heap free after GC to avoid shrinking.
#   (example: "40")
# - GC_TIME_RATIO: Specifies the ratio of the time spent outside the garbage collection.
#   (example: "4")
# - GC_ADAPTIVE_SIZE_POLICY_WEIGHT: The weighting given to the current GC time versus
#   previous GC times. (example: "90")
# - GC_METASPACE_SIZE: The initial metaspace size. (example: "20")
# - GC_MAX_METASPACE_SIZE: The maximum metaspace size. (example: "100")
# - GC_CONTAINER_OPTIONS: Specify Java GC to use. The value of this variable should
#   contain the necessary JRE command-line options to specify the required GC, which
#   will override the default of `-XX:+UseParallelGC` (example: -XX:+UseG1GC).
# - HTTPS_PROXY: The location of the https proxy. (example: "myuser@127.0.0.1:8080")
# - HTTP_PROXY: The location of the http proxy. (example: "myuser@127.0.0.1:8080")
# - NO_PROXY: A comma separated lists of hosts, IP addresses or domains that can be
#   accessed directly. (example: "foo.example.com,bar.example.com")
#
###
FROM gradle:8.8.0-jdk21 as builder



WORKDIR /home/gradle/work

# Copy the project files
COPY --chown=gradle:gradle flow-jvm/gradle gradle
COPY --chown=gradle:gradle flow-jvm/*.gradle flow-jvm/gradle.properties ./

# Build once to cache dependencies
RUN gradle build -x quarkusGenerateCode -x quarkusGenerateCodeDev  --stacktrace --info

# Gathers all proto files
COPY --chown=gradle:gradle protos/*.proto ../protos/
# Adds any extra configuration files if needed for build
COPY --chown=gradle:gradle flow-jvm/src/main/resources ./src/main/resources

# Only generate the code from the proto files
RUN gradle quarkusGenerateCode -x quarkusGenerateCodeDev --stacktrace --info


COPY --chown=gradle:gradle flow-jvm .
# Finally build the project
RUN gradle build --stacktrace --info


FROM registry.access.redhat.com/ubi8/openjdk-21:1.19 as runner



ENV LANGUAGE='en_US:en'


# We make four distinct layers so if there are application changes the library layers can be re-used
COPY --from=builder --chown=185 /home/gradle/work/build/quarkus-app/lib/ /deployments/lib/
COPY --from=builder --chown=185 /home/gradle/work/build/quarkus-app/*.jar /deployments/
COPY --from=builder --chown=185 /home/gradle/work/build/quarkus-app/app/ /deployments/app/
COPY --from=builder --chown=185 /home/gradle/work/build/quarkus-app/quarkus/ /deployments/quarkus/

# GRPC Port can be changed via FLOW_JVM_PORT env var
EXPOSE 9801
USER 185
ENV JAVA_OPTS_APPEND="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"
ENV PEERDB_LOG_LEVEL="INFO"

ENTRYPOINT [ "/opt/jboss/container/java/run/run-java.sh" ]

