#!/bin/bash

# telepítés, ha még nem létezik ilyen szervíz 

if systemctl list-units --type=service | grep -q "mongo"; then
  echo "MongoDB service is installed"
else
  echo "MongoDB service is not installed"
  # MongoDB szervíz telepítése a hivatalos oldalról
    sudo apt update && sudo apt upgrade
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
    curl -sSL https://www.mongodb.org/static/pgp/server-6.0.asc  -o mongoserver.asc
    gpg --no-default-keyring --keyring ./mongo_key_temp.gpg --import ./mongoserver.asc
    gpg --no-default-keyring --keyring ./mongo_key_temp.gpg --export > ./mongoserver_key.gpg
    sudo mv mongoserver_key.gpg /etc/apt/trusted.gpg.d/
    sudo apt update
    sudo apt install mongodb-org
    sudo systemctl enable --now mongod
    sudo systemctl status mongod

fi

echo
echo "MongoDB installed... wait"
echo 

# ha az alapértelmezett könyvtárban tároljuk az adatbázist akkor írjuk át
if grep -q "dbPath: \/var\/lib\/mongodb" /etc/mongod.conf
then
    # Update dbPath to /var/db/mongodb
    sed -i 's/dbPath: \/var\/lib\/mongodb/dbPath: \/var\/db\/mongodb/g' /etc/mongod.conf
    echo "dbPath updated in /etc/mongod.conf"
    sudo systemctl restart mongod
fi

# várakozás mert egyébként problémás a túl gyakori újraindítás
sleep 5 


# adatbázis könyvtárak létrehozása, jogosultságok beállítása
sudo mkdir -p /var/db/mongodb
sudo chown -R mongodb:mongodb /var/db/mongodb
sudo chmod -R 755 /var/db/mongodb
sudo chmod a+rw /var/db/mongodb
sudo rsync -av /var/lib/mongodb /var/db/mongodb --chown=mongodb:mongodb
sudo sysctl -w vm.max_map_count=524288
sudo sysctl -p

# szervíz újraindítása
sudo systemctl restart mongod