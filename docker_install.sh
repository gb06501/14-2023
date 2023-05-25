#!/bin/bash
sudo apt install docker.io
sudo groupadd docker
sudo usermod -aG docker ubuntu
newgrp docker
docker run hello-world