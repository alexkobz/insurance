FROM python:3.12
LABEL authors="alexkobz"

RUN apt-get update && apt-get -y install cron
RUN pip install jupyter ipykernel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crontab /etc/cron.monthly/crontab
COPY data/output data/output
COPY src src
COPY ratings.ipynb .
COPY prices.ipynb .
COPY stocks.ipynb .

RUN mkdir "logs"
RUN chmod +x /etc/cron.monthly/crontab
RUN chmod +x *.ipynb
RUN touch /var/log/cron.log
