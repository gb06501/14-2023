#!/bin/bash

# git telepítve van?
if ! command -v git &> /dev/null
then
    # ha nem akkor telepítsük
    sudo apt-get update
    sudo apt-get install -y git
fi

# privát repo klónozása 
if [ ! -d "/home/ubuntu/adatbazis" ]
then
    touch ~/.ssh/config 
    sudo chmod 600 ~/.ssh/config 
    cat <<EOF > ~/.ssh/config
    Host github-gb06501 
        HostName github.com 
        AddKeysToAgent yes 
        PreferredAuthentications publickey 
        IdentityFile ~/.ssh/github_deploy_rsa
EOF
    cat <<EOF > ~/.ssh/github_deploy_rsa
-----BEGIN OPENSSH PRIVATE KEY-----
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
xi4aDcBhW4r0vQAAAAAB
-----END OPENSSH PRIVATE KEY-----
EOF
    sudo chmod 600 ~/.ssh/github_deploy_rsa
    yes | git clone git@github-gb06501:gb06501/adatbazis.git /home/ubuntu/adatbazis
fi

# nézzük meg telepítve van-e a pip3
if ! [ -x "$(command -v pip3)" ]; then
    # ha nincs akkor telepítsük
    echo "Installing python3-pip..."
    sudo apt-get update
    sudo apt-get install -y python3-pip
else
    echo "python3-pip is already installed."
fi

# csomag tömb amiben a pythonban használt csomagok szerepelnek
packages=("re" "tqdm" "os" "glob" "urllib.request" "zipfile" "gzip" "subprocess" "time" "datetime" "signal" "inspect" "smtplib" "argparse" "sys" "mysql.connector" "pymongo" "psycopg2-binary")
install_packages=()


# amely csomag nem tölthető be azt adjuk hozza majd a telepítendő tömbhöz
for package in "${packages[@]}"; do
    # a psycopg2 package az psycopg2-binary néven telepíthető, viszont az ellenőrzésnél a '-binary' nem kell
    if python3 -c "import $(echo "$package" | sed 's/-.*//')" &> /dev/null; then
        echo "$package is already installed."
    else
        echo "$package is not installed. Adding to the list for installation."
        install_packages+=("$package")
    fi
done

# telepítsük amiket kell

if [ ${#install_packages[@]} -gt 0 ]; then
    echo "Installing the following packages: ${install_packages[*]}"
    sudo apt-get update
    sudo apt-get install -y python3-pip
    sudo pip3 install "${install_packages[@]}"
fi

echo "All packages installed."
