#!/bin/bash
# Rollback: belirtilen image tag'ine geri dön
# Kullanım: ./deployment/rollback.sh sha-abc1234

set -e

TARGET_TAG=${1:-"latest"}
IMAGE="ghcr.io/m-karakus/thelook"

echo "🔄 Rolling back to: $IMAGE:$TARGET_TAG"

# docker-compose.yaml'daki image tag'ini güncelle
sed -i "s|$IMAGE:.*|$IMAGE:$TARGET_TAG|g" docker-compose.yaml

# Servisleri yeniden başlat
docker compose pull
docker compose up -d --remove-orphans

echo "✅ Rollback to $TARGET_TAG complete"
docker compose ps
