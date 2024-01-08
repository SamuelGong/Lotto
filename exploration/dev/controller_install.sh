#!/bin/bash

pip install -q paramiko

# optionally do it if having sudo access
if groups | grep "\<sudo\>" &> /dev/null; then
   sudo timedatectl set-timezone Asia/Hong_Kong
fi

ORIGINAL_DIR=$(pwd)

cd `dirname $0`
WORKING_DIR=$(pwd)

# need to change according to the relative location of this script
PROJECT_DIR=${WORKING_DIR}/../..
echo $PROJECT_DIR

# install anaconda if necessary
CONDA_DIR=${HOME}/anaconda3
if [ ! -d ${CONDA_DIR} ]; then
  echo "[INFO] Install Anaconda Package Manager..."
  cd ~
  wget https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
  bash Anaconda3-2020.11-Linux-x86_64.sh -b -p ${CONDA_DIR}
  export PATH=${CONDA_DIR}/bin:$PATH
  rm Anaconda3-2020.11-Linux-x86_64.sh
  conda init bash
else
  echo "[INFO] Anaconda already installed."
fi

source ~/anaconda3/etc/profile.d/conda.sh

# cannot even proceed if not having sudo privilege
if ! groups | grep "\<sudo\>" &> /dev/null; then
   echo "[FAILED] You need to have sudo privilege."
   exit -1
fi

# for shared memory
if ! which redis-server > /dev/null 2>&1; then
    sudo apt update
    sudo apt install redis-server -y
fi

cd ${PROJECT_DIR}
pip install -r requirements.txt

cd ${ORIGINAL_DIR}
