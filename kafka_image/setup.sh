#!/bin/bash

BUILD=0
RUN=0
INSTALL=0

if [[ $# -eq 0 ]]; then
  BUILD=1
  RUN=1
  INSTALL=1
fi

while test $# -gt 0; do
        case "$1" in
                -h|--help)
                        echo "options:"
                        echo "-b      build docker image"
                        echo "-i      install docker"
                        echo "-r      run image with name kafka_dani"
			echo "default parameters: -b -i -r"
                        exit 0
                        ;;
                -b)
                        BUILD=1
                        shift
                        ;;
                -r)
                        RUN=1
                        shift
                        ;;
                -i)
                        INSTALL=1
                        shift
                        ;;
                *)
                        break
                        ;;
        esac
done

if [[ $INSTALL -eq 1 ]]; then
  echo 'Downloading docker...'
  curl -sSL https://get.docker.com/ | sh
  echo 'Adding current user to docker group'
  sudo usermod -aG docker $USER
  echo "Applying group settings, type the password for the user $USER if needed:"
  su - $USER
fi

if [[ $BUILD -eq 1 ]]; then
  echo 'Building image...'
  docker build -t="nventdata/kafka_dani" .
fi

if [[ $RUN -eq 1 ]]; then
  echo 'Starting instance of the built image...'
  docker run --name kafka_dani -p 9092:9092 -p 2181:2181 -t "nventdata/kafka_dani"
fi
