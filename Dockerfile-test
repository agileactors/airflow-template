FROM apache/airflow:2.5.1-python3.10
ADD requirements.txt .
RUN pip install -r requirements.txt

#odbc

USER root

# Debian
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

#Download appropriate package for the OS version
#Choose only ONE of the following, corresponding to your OS version

#Debian 11
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list


RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install msodbcsql18