FROM python:3.9 AS build
COPY ./requirements.txt /requirements.txt
RUN pip install -r ./requirements.txt
RUN rm /requirements.txt

FROM build AS prod 
COPY . /importMovieDatabase 
ENV PYTHONPATH=/importMovieDatabase
WORKDIR /importMovieDatabase