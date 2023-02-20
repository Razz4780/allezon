#!/bin/bash

set -e

if [[ $# -ne 2 ]]; then
    echo "USAGE: install.sh [ansible_user] [ansible_password]"
    exit 1
fi

if [[ ! -f /usr/bin/ansible-playbook ]]; then
    add-apt-repository -y  ppa:ansible/ansible
    apt update
fi

apt install -y cmake libssl-dev pkg-config sshpass ansible

if [[ ! -f ~/.cargo/bin/cargo ]]; then
    wget https://sh.rustup.rs -O rustup-init.sh
    echo "41262c98ae4effc2a752340608542d9fe411da73aca5fbe947fe45f61b7bd5cf  rustup-init.sh" | sha256sum --check
    sh rustup-init.sh -y
fi

. ~/.cargo/env
cargo build --release
install target/release/api_server ansible/api_server/
install target/release/consumer ansible/consumer/

cd ansible
sed "s/XXX/$1/g" hosts.yaml > hosts_tmp.yaml
ansible-playbook --extra-vars "ansible_user=$1 ansible_password=$2 ansible_ssh_extra_args='-o StrictHostKeyChecking=no'" -i hosts_tmp.yaml allezon.yaml
