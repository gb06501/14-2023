#!/bin/bash

# telepítsük a PostgreSQL 14
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql-14

# állítsuk le a szervízt
sudo systemctl stop postgresql@14-main.service

# ha már létezik telepített könyvtár akkor nevezzük át
sudo mv /var/lib/postgresql/14/main /var/lib/postgresql/14/main.bak

# új adat könyvtár létrehozása
sudo mkdir -p /var/db/postgres
sudo chown -R postgres:postgres /var/db/postgres
sudo chmod 700 /var/db/postgres

sudo mkdir -p /var/db/csv_store/postgres
sudo chown -R postgres:postgres /var/db/csv_store/postgres
sudo chmod 700 /var/db/postgres


# új klaszter létrehozása
sudo -u postgres /usr/lib/postgresql/14/bin/initdb -D /var/db/postgres

# az adatkönyvtár átírása a konfigurációs fájlban
sudo sed -i "s|/var/lib/postgresql/14/main|/var/db/postgres|" /etc/postgresql/14/main/postgresql.conf

# indítsuk el a PostgreSQL szervizt.
sudo systemctl start postgresql@14-main.service
