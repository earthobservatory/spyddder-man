FROM hysds/pge-base:latest

MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="spyddder man"

# copy packages
COPY . /home/ops/verdi/ops/spyddder-man

# as ops user
USER ops
WORKDIR /home/ops

CMD ["/bin/bash", "--login"]
