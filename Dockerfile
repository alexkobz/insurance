FROM python:3.8
LABEL authors="alexkobz"

RUN apt-get update && apt-get -y install cron
RUN pip install jupyter ipykernel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crontab /etc/cron.monthly/crontab
COPY data/Output data/Output
COPY functions functions
COPY logger logger
COPY rudata rudata
COPY ratings.ipynb .
COPY cash_flow.ipynb .

RUN chmod +x /etc/cron.monthly/crontab
RUN chmod +x *.ipynb
RUN touch /var/log/cron.log
