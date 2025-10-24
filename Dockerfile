FROM python:3.10
LABEL authors="alexkobz"

RUN apt-get update && apt-get -y install cron
RUN pip install jupyter ipykernel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crontab /etc/cron.monthly/crontab
COPY data/Output data/Output
COPY src/utils functions
COPY src/sources/rudata rudata
COPY notebooks/ratings.ipynb .
COPY notebooks/prices.ipynb .
COPY notebooks/stocks.ipynb .

RUN mkdir "logs"
RUN chmod +x /etc/cron.monthly/crontab
RUN chmod +x *.ipynb
RUN touch /var/log/cron.log
