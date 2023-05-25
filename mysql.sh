#!/bin/bash

# Stop MySQL service
sudo systemctl stop mysql

# Uninstall MySQL
sudo apt-get remove --purge mysql-server mysql-client mysql-common -y
sudo apt-get autoremove -y
sudo apt-get autoclean

# Install MySQL
sudo apt-get install mysql-server -y

echo "Server should have been up for now, wait 10s"
sleep 10s
sudo systemctl stop mysql
    # Backup and move existing data directory
if [ -d "/var/lib/mysql/mysql" ] && [ ! "$(ls -A /var/lib/mysql/mysql)" ]; then
    echo "Copying /var/lib/mysql.bak to /var/db..."
    sudo rsync -av /var/lib/mysql.bak /var/db/mysql --chown=mysql:mysql
    sudo  mv /var/db/mysql/mysql.bak /var/db/mysql/mysql
else
        sudo rsync -av /var/lib/mysql.bak /var/db/mysql --chown=mysql:mysql
        sudo mv /var/db/mysql/mysql.bak /var/lib/mysql/mysql
fi

    # Configure MySQL
    #sudo sed -i 's/^#*datadir=.*/datadir=\/var\/db\/mysql/' /etc/mysql/mysql.conf.d/mysqld.cnf
    sudo sed -i '/datadir/{N;d}; $a\datadir=\/var\/db\/mysql\/mysql' /etc/mysql/mysql.conf.d/mysqld.cnf

    # Add alias to AppArmor
if ! grep -q '^alias /var/lib/mysql/ -> /var/db/mysql/mysql/,' /etc/apparmor.d/tunables/alias; then
    echo 'alias /var/lib/mysql/ -> /var/db/mysql/mysql/,' | sudo tee -a /etc/apparmor.d/tunables/alias
    sudo systemctl restart apparmor
fi

    # Create new data directory
    sudo mkdir -p /var/lib/mysql/mysql -p
    sudo chown mysql:mysql /var/lib/mysql/mysql /var/lib/mysql /var/db/mysql /var/db/mysql/mysql
    sudo chmod 1777 /var/lib/mysql/mysql /var/lib/mysql /var/db/mysql /var/db/mysql/mysql


# Check if require_secure_transport is present and set to OFF
if grep -q '^require_secure_transport[[:space:]]*=[[:space:]]*OFF' /etc/mysql/mysql.conf.d/mysqld.cnf; then
    echo "Setting already exists and is set to OFF."
else
    if grep -q '^require_secure_transport' /etc/mysql/mysql.conf.d/mysqld.cnf; then
        sed -i 's/^require_secure_transport.*/require_secure_transport = OFF/' /etc/mysql/mysql.conf.d/mysqld.cnf
        RESTART_MYSQL=true
    else
        echo "require_secure_transport = OFF" | sudo tee -a /etc/mysql/mysql.conf.d/mysqld.cnf
        RESTART_MYSQL=true
    fi
fi

if grep -q '\[mysqld\]' /etc/mysql/my.cnf; then
  # Check if the local_infile option exists in the [mysqld] section
  if ! grep -q 'local_infile' /etc/mysql/my.cnf; then
    # Add the local_infile option to the [mysqld] section
    sed -i '/\[mysqld\]/a local_infile=ON' /etc/mysql/my.cnf
  fi
else
  # Add the [mysqld] section and the local_infile option to the my.cnf file
  echo '[mysqld]' >> /etc/mysql/my.cnf
  echo 'local_infile=ON' >> /etc/mysql/my.cnf
fi


if [ "$RESTART_MYSQL" = true ]; then
    echo "Restarting MySQL..."
    systemctl restart mysql
    systemctl status mysql
    echo "MySQL restarted. Wait 10s"
    sleep 10
else
    echo "No changes made to MySQL configuration file."
fi
RESTART_MYSQL=false

if ! dpkg -s python3-mysql.connector >/dev/null 2>&1; then
    echo "python3-mysql.connector is not installed. Installing now..."
    sudo apt install python3-mysql.connector
else
    echo "python3-mysql.connector is already installed."
fi


# Restart MySQL if the setting was changed


if sudo mysql -e "SELECT User FROM mysql.user;" | grep -q '^ubuntu$'; then
    echo "User 'ubuntu' exists in MySQL"
else
    echo "User 'ubuntu' does not exist in MySQL"
    mysql -e "CREATE USER 'ubuntu'@'localhost' IDENTIFIED BY ''; GRANT ALL PRIVILEGES ON *.* TO 'ubuntu'@'localhost';"
    mysql -e "GRANT FILE ON *.* TO 'ubuntu'@'localhost';"
    #mysql -e "SET GLOBAL local_infile = 'ON';"
    RESTART_MYSQL=true
    echo "User 'ubuntu' created, MySQL restarted."
fi

# Start MySQL service


if [ "$RESTART_MYSQL" = true ]; then
    echo "Restarting MySQL..."
    systemctl restart mysql
    systemctl status mysql
    echo "MySQL restarted."
else
    echo "No changes made to MySQL configuration file."
fi



