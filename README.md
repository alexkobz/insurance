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

   * По расписанию 
   ```bash 
   docker compose up -f docker-compose.yml
   ```
   * Вручную
   ```bash 
   docker compose up -f docker-compose-manual.yml
   ```
   Контейнер с ch всегда должен быть running при запуске


NB 
* Файлы отправляются на почту. LOGIN_EMAIL и PASSWORD_EMAIL в .env файле
* Везде в качестве даты стоит последний день месяца last_day_month, кроме выгрузок из Moex - обязателен последний рабочий день месяца last_work_date_month
