# Required GitHub Secrets

Go to: Settings → Secrets and variables → Actions → New repository secret

| Secret Name       | Description                                      |
|-------------------|--------------------------------------------------|
| GHCR_TOKEN        | GitHub PAT with `write:packages` scope           |
| EC2_HOST          | EC2 public IP or DNS                             |
| EC2_SSH_KEY       | EC2 private key (PEM format, full content)       |
| EC2_USER          | EC2 SSH user (ubuntu or ec2-user)                |
| ENV_FILE          | Full content of production .env file             |
| CREDENTIALS_YAML  | Full content of production .credentials.yaml     |
