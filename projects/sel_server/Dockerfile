FROM python:3.6

ARG PROJECT
ENV PROJECT="${PROJECT}"


ADD projects/sel/sel /src/sel
ADD projects/sel/setup.py .
ADD projects/sel/setup.cfg .
RUN pip3 install -e .


ADD projects/${PROJECT}/${PROJECT} /src/${PROJECT}
ADD projects/${PROJECT}/docker-entrypoint.sh .
ADD projects/${PROJECT}/conf.ini .
ADD projects/${PROJECT}/setup.py .
ADD projects/${PROJECT}/setup.cfg .
RUN pip3 install -e .

ENV PYTHONPATH $PYTHONPATH:/src
ENV ES_HOST http://elasticsearch
EXPOSE 9000

ENTRYPOINT ["./docker-entrypoint.sh"]
