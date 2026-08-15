FROM apache/nifi:1.26.0

COPY extensions/*.jar /opt/nifi/nifi-current/lib/
