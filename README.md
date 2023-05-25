# 14-2023

A mongo / mysql / postgres könyvtárban találhatóak a forgatókönyv alapján futtatott natív és Docker tesztek.

## Forgatókönyv

A teszteket az alábbi módszerrel futtattam:

MongoDB Natív:
```sh
nohup python3 -u mongo_script.py > ../mongo/local/mongo_local.out &
```
MongoDB Docker:
```sh
nohup python3 -u mongo_script.py --docker > ../mongo/docker/mongo_docker.out &
```
PostgreSQL Natív:
```sh
nohup python3 -u postgres_script.py > ../postgres/local/postgres_local.out &
```
PostgreSQL Docker:
```sh
nohup python3 -u postgres_script.py --docker > ../postgres/docker/postgres_docker.out &
```
MySQL Natív:
```sh
nohup python3 -u mysql_script.py > ../mysql/local/mysql_local.out &
```
MySQL Docker:
```sh
nohup python3 -u mysql_script.py --docker > ../mysql/docker/mysql_docker.out &
```


[MongoDB mongo_script.py](mongo_script.py)  
[PostgreSQL postgres_script.py](postgres_script.py)  
[MySQL mysql_script.py](mysql_script.py)  

## Modulok

[MongoDB mongo_functions modul](mongo_functions.py)  
[PostgreSQL postgres_functions modul](postgres_functions.py)  
[MySQL mysql_functions modul](mysql_functions.py)  

## Jegyzőkönyv

[MongoDB Natív jegyzőkönyv](mongo/local/mongo_local.out)  
[MongoDB Docker jegyzőkönyv](mongo/docker/mongo_docker.out)  
[PostgreSQL Natív jegyzőkönyv](postgres/local/postgres_local.out)  
[PostgreSQL Docker jegyzőkönyv](postgres/docker/postgres_docker.out)  
[MySQL Natív jegyzőkönyv](mysql/local/mysql_local.out)  
[MySQL Docker jegyzőkönyv](mysql/docker/mysql_docker.out)  








