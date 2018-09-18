#registry.hub.docker.com/
FROM library/cassandra:3.11
ENV DEF_USER=cassandra
ENV DEF_PASS=cassandra
COPY runcql.sh /
RUN mkdir -p cql/extra && \
    chmod 755 runcql.sh && \
    chown cassandra runcql.sh
CMD ["/runcql.sh"]
