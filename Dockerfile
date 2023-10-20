FROM python:3.6

ADD sel /sel
ADD setup.py .
ADD setup.cfg .
RUN pip3 install -e .

ENV ES_HOST http://elasticsearch
