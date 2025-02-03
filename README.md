## Prerequisites

Before diving into the packaging process, you'll need:

- Python 3.10 or later installed on your machine.
- All the necessary dependencies listed in requirements.txt
To install these requirements, run the following commands:

```bash
# Upgrade pip to the latest version
python -m pip install -U pip

# Install the required packages
pip install -r requirements.txt
```

## Краткая инструкция

Есть 2 сервиса:

1. Выгрузка ratings (ratings.ipynb) и cash_flow и stocks_data (stocks.ipynb)
   * По расписанию 
   ```bash 
   docker compose up -f docker-compose-schedule.yml
   ```
   * Вручную
   ```bash 
   docker compose up -f docker-compose-manual.yml
   ```
   Контейнер с postgres всегда должен быть running при запуске

2. Выгрузка prices (prices.ipynb) всегда только вручную через jupyter, так как перед каждым запуском нужно получить файлы
   ```bash 
   jupyter execute prices.ipynb
   ```

NB 
* Файлы отправляются на почту. LOGIN_EMAIL и PASSWORD_EMAIL в .env файле
* Везде в качестве даты стоит последний день месяца last_day_month, кроме выгрузок из Moex - обязателен последний рабочий день месяца last_work_date_month
