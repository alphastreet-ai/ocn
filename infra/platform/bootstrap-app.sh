#!/usr/bin/env bash
# Runs roles.sql on the alphastreet DB through the bastion (SSM port forward) and stores svc_app's
# DATABASE_URL in Secrets Manager for alphaStreet-api. Passwords never touch the terminal or
# history, nor any process's argv. Rerunnable: it rotates svc_app's password and the secret together (restart the API after).
# Needs: AWS_PROFILE, session-manager-plugin, psql, and the bastion in client_sg_ids.
set -euo pipefail
cd "$(dirname "$0")"
command -v session-manager-plugin >/dev/null || { echo "Install it: brew install --cask session-manager-plugin" >&2; exit 1; }

ENV=${ENV:-staging}
REGION=${AWS_REGION:-$(aws configure get region)}
# Exactly one running bastion, or name it with BASTION=i-...: never guess which host carries the tunnel.
if [ -z "${BASTION:-}" ]; then
  BASTIONS=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=${ENV}-bastion*" \
    "Name=instance-state-name,Values=running" --query 'Reservations[].Instances[].InstanceId' --output text)
  [ "$(echo "$BASTIONS" | wc -w)" -eq 1 ] || { echo "Expected one ${ENV}-bastion*, found: ${BASTIONS:-none}. Set BASTION=i-..." >&2; exit 1; }
  BASTION=$BASTIONS
fi
SECRET_NAME="alphastreet/${ENV}/db/svc_app"
LOCAL_PORT=15432
HOST=$(terraform output -raw endpoint)
MASTER_SECRET=$(terraform output -raw master_user_secret_arn)

aws ssm start-session --target "$BASTION" --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters "host=$HOST,portNumber=5432,localPortNumber=$LOCAL_PORT" >/dev/null &
TUNNEL=$!
umask 077
TMP=$(mktemp -d)
trap 'kill $TUNNEL 2>/dev/null; rm -rf "$TMP"' EXIT
for _ in $(seq 30); do nc -z localhost $LOCAL_PORT 2>/dev/null && break; sleep 1; done
nc -z localhost $LOCAL_PORT 2>/dev/null || { echo "SSM tunnel to $BASTION did not open" >&2; exit 1; }

# verify-full through the tunnel: connect to 127.0.0.1 but check the cert against the RDS name.
curl -fsS "https://truststore.pki.rds.amazonaws.com/${REGION}/${REGION}-bundle.pem" -o "$TMP/rds-ca.pem"
PGPASSWORD=$(aws secretsmanager get-secret-value --secret-id "$MASTER_SECRET" \
  --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)["password"])')
export PGPASSWORD
SVC_APP_PASSWORD=$(openssl rand -hex 24)
export SVC_APP_PASSWORD # roles.sql reads it with \getenv

psql "hostaddr=127.0.0.1 host=$HOST port=$LOCAL_PORT dbname=alphastreet user=master sslmode=verify-full sslrootcert=$TMP/rds-ca.pem" \
  -v ON_ERROR_STOP=1 -q -f roles.sql

printf '%s' "postgresql://svc_app:${SVC_APP_PASSWORD}@${HOST}:5432/alphastreet?sslmode=require" >"$TMP/url"
if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
  aws secretsmanager put-secret-value --secret-id "$SECRET_NAME" --secret-string "file://$TMP/url" >/dev/null
else
  aws secretsmanager create-secret --name "$SECRET_NAME" --secret-string "file://$TMP/url" \
    --description "alphaStreet-api DATABASE_URL (svc_app on the alphastreet DB)" >/dev/null
fi

echo "roles.sql applied; DATABASE_URL stored in $SECRET_NAME:"
aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --query ARN --output text
