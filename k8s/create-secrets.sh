#!/bin/bash
# k8s/create-secrets.sh — run once on the K3S node

read -rp "GitHub username: " GH_USER
read -rsp "GitHub PAT: " GH_PAT && echo
read -rsp "Cloudflare API token: " CF_TOKEN && echo

kubectl create secret docker-registry ghcr-pull-secret \
  --docker-server=ghcr.io \
  --docker-username="$GH_USER" \
  --docker-password="$GH_PAT"

kubectl create secret generic cf-radar-secret \
  --from-literal=CF_API_TOKEN="$CF_TOKEN"

echo "Done. Verify with: kubectl get secrets"