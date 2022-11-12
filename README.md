#BLUE ONION LABS TAKE HOME
Starlink data manipulation

## Tech: 
1. Python 3.8
2. Docker 0.19
3. Pip 21.1.2

## How to run?
* Clone and create a virtual environment. 
* Install requirements:
```console
$ pip install -r requirements.txt
```
* Set up Postgres with Docker:
```console
$ docker compose up
```
* Execute and follow instructions:
```console
$ python main.py
```

## Details: 
* Query to get position by satellite id and creation date.
```sql
SELECT latitude, longitude FROM starlink WHERE id='{id}' and creation_date='{time}';
```
* Query to get satellite positions by creation date.
```sql
SELECT id, latitude, longitude FROM starlink WHERE creation_date='{time}';
```