FROM python:3.12-slim

WORKDIR /backend/

ADD requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt
