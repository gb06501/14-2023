#!/bin/bash

# "data" tipusú lemezek keresése amik nem formázottak és/vagy nincsenek csatolva
for dev in $(lsblk -n -o NAME,TYPE | grep "disk" | awk '{print $1}' | grep "^nvme[0-9]n[0-9]$")
do
  fstype=$(lsblk -n -o FSTYPE /dev/$dev)
  mnt=$(lsblk -n -o MOUNTPOINT /dev/$dev)

  if [[ -z "$fstype" ]]
  then
    # Lemez nincs formázva
    echo "Formatting $dev as ext4"
    mkfs.ext4 /dev/$dev
  fi

  if [[ -z "$mnt" ]]
  then
    # Lemez nincs felcsatolva.
    echo "Mounting $dev to /var/db"
    mkdir -p /var/db
    mount /dev/$dev /var/db
    if grep -qs "/dev/$dev" /etc/fstab; then
      echo "/dev/$dev is already in /etc/fstab. Skipping."
    else
      # Eszköz felvétele /etc/fstab fájlba
      echo "/dev/$dev /var/db ext4 defaults,nofail 0 2" >> /etc/fstab
    fi
  fi
done

# aws efs hálózati lemez csatolása
sudo mkdir -p /var/db/csv_store
sudo chown ubuntu:ubuntu /var/db/csv_store
sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport fs-01221aec0bf32eb9b.efs.eu-central-1.amazonaws.com:/ /var/db/csv_store/

