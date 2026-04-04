#!/bin/bash
# EC2 ilk kurulum scripti — bir kez çalıştırılır
# Kullanım: bash ec2-setup.sh

set -e

echo "🚀 Setting up EC2 instance for theLook pipeline..."

# Docker kur
sudo apt-get update -y
sudo apt-get install -y docker.io docker-compose-plugin git curl

# Docker daemon başlat
sudo systemctl enable docker
sudo systemctl start docker

# Kullanıcıyı docker grubuna ekle
sudo usermod -aG docker $USER

# Repo klonla
git clone https://github.com/m-karakus/theLook.git ~/thelook
cd ~/thelook

echo "✅ EC2 setup complete."
echo "📋 Next steps:"
echo "   1. Copy .env and .credentials.yaml to ~/thelook/"
echo "   2. Run: docker compose up -d"
