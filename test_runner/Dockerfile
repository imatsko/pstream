FROM ubuntu:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends iproute

ADD run.sh /bin/run.sh
RUN chmod a+x /bin/run.sh

ENTRYPOINT ["/bin/run.sh"]

