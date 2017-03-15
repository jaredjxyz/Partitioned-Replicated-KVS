FROM ubuntu:latest
MAINTAINER Asha
RUN ["apt-get", "update", "-y"]
RUN ["apt-get", "install", "-y", "python-pip", "python-dev"]
RUN ["pip", "install", "django"]
RUN ["pip", "install", "djangorestframework"]
RUN ["pip", "install", "requests"]
COPY ./lab4 /lab4
EXPOSE 8080
WORKDIR /lab4
RUN ["python", "manage.py", "migrate"]
CMD ["python", "manage.py", "runserver", "0.0.0.0:8080"]
