#!/usr/bin/env bash
# Runs roles.sql on the platform DB through the bastion (SSM port forward) and stores svc_app's
# DATABASE_URL in Secrets Manager for alphaStreet-api. Passwords never touch the terminal or
# history. Rerunnable: it rotates svc_app's password and the secret together (restart the API after).
# Needs: AWS_PROFILE, session-manager-plugin, psql, and the bastion in client_sg_ids.
set -euo pipefail
cd "$(dirname "$0")"
command -v session-manager-plugin >/dev/null || { echo "Install it: brew install --cask session-manager-plugin" >&2; exit 1; }

ENV=${ENV:-staging}
BASTION=${BASTION:-$(aws ec2 describe-instances --filters "Name=tag:Name,Values=${ENV}-bastion*" \
  "Name=instance-state-name,Values=running" --query 'Reservations[0].Instances[0].InstanceId' --output text)}
SECRET_NAME="alphastreet/${ENV}/db/svc_app"
LOCAL_PORT=15432
HOST=$(terraform output -raw endpoint)
MASTER_SECRET=$(terraform output -raw master_user_secret_arn)

aws ssm start-session --target "$BASTION" --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "host=$HOST,portNumber=5432,localPortNumber=$LOCAL_PORT" >/dev/null &
TUNNEL=$!
trap 'kill $TUNNEL 2>/dev/null' EXIT
for _ in $(seq 30); do nc -z localhost $LOCAL_PORT 2>/dev/null && break; sleep 1; done
nc -z localhost $LOCAL_PORT 2>/dev/null || { echo "SSM tunnel to $BASTION did not open" >&2; exit 1; }

# sslmode=require (not verify-full): the tunnel makes the host localhost, so the cert name can't match.
PGPASSWORD=$(aws secretsmanager get-secret-value --secret-id "$MASTER_SECRET" \
  --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["password"])')
export PGPASSWORD
SVC_APP_PASSWORD=$(openssl rand -hex 24)

psql "host=localhost port=$LOCAL_PORT dbname=platform user=master sslmode=require" \
  -v ON_ERROR_STOP=1 -v svc_app_password="$SVC_APP_PASSWORD" -q -f roles.sql

URL="postgresql://svc_app:${SVC_APP_PASSWORD}@${HOST}:5432/platform?sslmode=require"
if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
  aws secretsmanager put-secret-value --secret-id "$SECRET_NAME" --secret-string "$URL" >/dev/null
else
  aws secretsmanager create-secret --name "$SECRET_NAME" --secret-string "$URL" \
    --description "alphaStreet-api DATABASE_URL (svc_app on the platform DB)" >/dev/null
fi

echo "roles.sql applied; DATABASE_URL stored in $SECRET_NAME:"
aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --query ARN --output text
