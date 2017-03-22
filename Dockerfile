FROM ubuntu:latest
FROM python:3.5
MAINTAINER Hunter hbingham@ucsc.edu
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev
RUN pip install Flask
RUN pip install requests
RUN pip install Flask-API
RUN pip install markdown
COPY ./webapp /app
WORKDIR /app
EXPOSE 8080
CMD python /app/app.py
