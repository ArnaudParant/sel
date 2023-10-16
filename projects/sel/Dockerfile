FROM python:3.6

ARG PROJECT
ENV PROJECT="${PROJECT}"

ADD projects/${PROJECT}/${PROJECT} /src/${PROJECT}
ADD projects/${PROJECT}/setup.py .
ADD projects/${PROJECT}/setup.cfg .
RUN pip3 install -e .

ENV PYTHONPATH $PYTHONPATH:/src
ENV ES_HOST http://elasticsearch
