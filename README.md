## Prerequisites

Before diving into the packaging process, you'll need:

- Python 3.8 or later installed on your machine.
- All the necessary dependencies listed in requirements.txt
To install these requirements, run the following commands:

```bash
# Upgrade pip to the latest version
python -m pip install -U pip

# Install the required packages
pip install -r requirements.txt
```

## Инструкция
### Запуск скрипта

Код находится в файле ratings.ipynb
Для запуска скрипта нужно запустить все ячейки в файле ratings.ipynb
Результат data/Output/ratings.xlsx
 
### RU DATA API является REST API веб-сервисом, предоставляющим доступ к информации по ценным бумагам из хранилища данных Интерфакс и НРД.

RU DATA API позволяет получить следующую информацию:
●	Данные по облигациям (параметры выпуска, купоны, оферты и др.);
●	Данные по акциям, депозитарным распискам, фондам и ИСУ;
●	Справки по эмитентам и их ценным бумагам;
●	Архивы и итоги торгов финансовыми инструментами на Московской бирже;
●	Рейтинги компаний и облигаций;

[Информация по методам и контроллерам](https://docs.efir-net.ru/dh2/#/)

[API RU DATA](https://dh2.efir-net.ru/swagger/index.html?urls.primaryName=DataHub%20v2.0)

Запросы рейтингов:

1.	requests.post('https://dh2.efir-net.ru/v2/Account/Login', json=payload,headers=headers)

Account - сервис авторизации
v2/Account/Login - авторизация пользователя. Получить авторизованный токен
Задает имя пользователя и пароль.
Выдает токен
[Свойства](https://docs.efir-net.ru/dh2/#/Account/Login)

2.	requests.post('https://dh2.efir-net.ru/v2/Info/FintoolReferenceData',json=payload,headers=rd_headers)

Info - Справочная информация по эмитентам и их ценным бумагам (акции, облигации, депозитарные расписки, фонды, ИСУ).

/v2/Info/FintoolReferenceData - получить расширенный справочник по финансовым инструментам

[Свойства](https://docs.efir-net.ru/dh2/#/Info/FintoolReferenceData)



3. requests.post('https://dh2.efir-net.ru/v2/Info/Emitents',json=payload,headers=rd_headers)

/v2/Info/Emitents - получить краткий справочник по эмитентам(организация, которая выпускает ценные бумаги для развития и финансирования своей деятельности)

[Свойства](https://docs.efir-net.ru/dh2/#/Info/Emitents)


4.	requests.post('https://dh2.efir-net.ru/v2/Bond/OfferorsGuarants',json=payload,headers=rd_headers)

Bond - база данных по облигациям

/v2/Bond/OfferorsGuarants - возвращает список гарантов(банк или страховая компания по отношению к принципалу, которому выдана банковская или страховая гарантия)/оферентов(фин. участник торгов, делающий предложение (оферту)) для инструмента

[Свойства](https://docs.efir-net.ru/dh2/#/Bond/OfferorsGuarants)


5.	requests.post('https://dh2.efir-net.ru/v2/Rating/SecurityRatingTable',json=payload,headers=rd_headers)

Rating - рейтинги компаний и облигаций

v2/Rating/SecurityRatingTable - получить рейтинги нескольких бумаг и связанных с ними компаний на заданную дату

[Свойства](https://docs.efir-net.ru/dh2/#/Rating/SecurityRatingTable)


6.	requests.post('https://dh2.efir-net.ru/v2/Rating/ListRatings',headers=rd_headers)

/v2/Rating/ListRatings - получить список рейтингов

[Свойства](https://docs.efir-net.ru/dh2/#/Rating/ListRatings)


7.	requests.post('https://dh2.efir-net.ru/v2/Rating/ListScaleValues',headers=rd_headers)

v2/Rating/ListScaleValues - список шкал(Международная и национальная) значений рейтингов
 

[Свойства](https://docs.efir-net.ru/dh2/#/Rating/ListScaleValues)

8.	requests.post('https://dh2.efir-net.ru/v2/Rating/CompanyRatingsTable',json=payload,headers=rd_headers)

/v2/Rating/CompanyRatingsTable - получить рейтинги нескольких компаний на заданную дату

[Свойства](https://docs.efir-net.ru/dh2/#/Rating/CompanyRatingsTable)



