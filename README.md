# Финальный проект на курсе Data Engineer 
# Ретроспективные данные по гонкам класса Формула 1

# Введение
Ссылка на презентацию к проекту: [link](https://docs.google.com/presentation/d/1X8iyODSMfG9wkJ2x2F6fJ3rYFK76FJcZ86E55kaeVns/edit?usp=sharing)

## Цели проекта
* Организация сбора, обработки и визуализации данных по гонкам класса Формула 1, представленных на портале [Ergast Developer API](https://ergast.com/mrd) (результаты гран-при, время круга, кол-во очков, поломки);
* Получение практического опыта в использовании инструментов инженера данных (SQL, Streamsets, pyspark, Vertica, docker, …).

## Основные этапы
1. Запуск docker контейнеров (mysql, pyspark-notebook, vertica), используя docker-compose.yml;
2. Развертка акутального образа БД в MySQL;
3. Трансформация сырых данных в pyspark;
4. Запись результатов в аналитическую БД Vertica;
5. Визуализация результатов при помощи Jupyter Notebook.

## Схема проекта
На рисунке ниже изображена принципиальная схема проекта:
![principal_schema](/support_data/pics/F1_retrospective_DB.png "project schema")

\* Звездочками и красными стрелками показаны задачи и инструменты планируемые к внедрению в проект.

# Использование проекта
## Необходимые инструменты
* docker + docker-compose
* Python 3.6+ (финальная визуализация через Jupyter notebook)
    * Jupyter notebook
    * vertica-python
    * verticapy
Версии пакетов указаны в [requirements.txt](https://github.com/Shevelsm/f1-retrospective-data/blob/master/requirements.txt)

## Запуск проекта 
Вначале необхоимо клонировать репозиторий проекта
```shell
    $ git clone git@github.com:Shevelsm/f1-retrospective-data.git
    $ cd f1-retrospective-data
```

Для финальной визуализации результатов в Jupyter notebook можно использовать виртуальное окружение для Python.

Для Linux/Mac:
```bash
    $ python3 -m venv venv
    $ . venv/bin/activate
```

Для Windows:
```cmd
    $ py -3 -m venv venv
    $ venv\Scripts\activate.bat
```
Далее установить все необходимые зависимости можно при попщи команды:
```bash
    pip install -r requirements.txt
```
Затем запускаем все контейнеры для работы при помощи команды (\* возможна необходимость использование утилиты sudo):
```bash
    $ docker-compose up
```
Запускаются 3 контейнера (mysql, pyspark, vertica)
```bash
Starting f1-retrospective-data_mysql_1            ... done
Starting f1-retrospective-data_pyspark-notebook_1 ... done
Starting f1-retrospective-data_vertica_1          ... done
```
После того как контейнеры запустятся запускаем ETL процессы. На текущий момент запуск этих процессов осуществляется простым Python скриптом:
```bash
    python run_project.py
```
Как только завершатся все процессы, будет запущен Jupyter notebook с примером визуализации простой аналитики при помощи пакета `veticapy`
Для примера приведена аналитика по пилоту, команде и нации пилота.
Значения можно изменить в первой ячейке итогового ноутбука:
```python
driver = "alonso"
constructor = "renault"
driver_nation = "Russian"
```
В итоге при помощи встроенных инструментов визуализации пакета `verticapy` в ноутбуке выводятся несколько чартов. 
Например количество гонок каждого пилота выбранной нации (на рисунке ниже русские пилоты):
![Example_nation](/support_data/pics/example_nation.png "Num races of pilot")
# Планы по развитию
* Внедрение оркестратора (возможно airflow);
* Сбор данных исторических данных о погоде на гоночной трассе;
* Использование календаря гонок для автоматического обновления данных;
* Более интересная аналитика и визуализация.