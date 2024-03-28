FROM ubuntu:latest

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    apt-get install -y openjdk-11-jdk

# Create a dedicated system user for Cassandra
RUN groupadd -r cassandra && useradd --no-log-init -r -g cassandra cassandra

# Download and install Cassandra

RUN wget -q https://dlcdn.apache.org/cassandra/4.1.4/apache-cassandra-4.1.4-bin.tar.gz && \
    tar -xzf apache-cassandra-4.1.4-bin.tar.gz && \
    mv apache-cassandra-4.1.4 /opt/cassandra && \
    rm apache-cassandra-4.1.4-bin.tar.gz

# Adjust ownership of Cassandra files
RUN chown -R cassandra:cassandra /opt/cassandra

# Expose the necessary ports
EXPOSE 7000 7001 7199 9042 9160

# Switch to the Cassandra user and start Cassandra
USER cassandra
CMD ["/opt/cassandra/bin/cassandra", "-f"]
