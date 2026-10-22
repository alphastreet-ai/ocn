#!/usr/bin/env bash
# Runs roles.sql on the alphastreet DB through the bastion (SSM port forward) and stores each service
# role's DATABASE_URL in Secrets Manager: svc_app (alphaStreet-api) and svc_sim (alphastreet-simulation).
# Passwords never touch the terminal or history, nor any process's argv. Rerunnable: it rotates both
# passwords and secrets together (restart the API after; the sim's next task picks it up).
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
SVC_SIM_PASSWORD=$(openssl rand -hex 24)
export SVC_APP_PASSWORD SVC_SIM_PASSWORD # roles.sql reads them with \getenv

psql "hostaddr=127.0.0.1 host=$HOST port=$LOCAL_PORT dbname=alphastreet user=master sslmode=verify-full sslrootcert=$TMP/rds-ca.pem" \
  -v ON_ERROR_STOP=1 -q -f roles.sql

# store_url <role> <password> <description>: upsert alphastreet/<env>/db/<role>.
store_url() {
  local name="alphastreet/${ENV}/db/$1"
  printf '%s' "postgresql://$1:$2@${HOST}:5432/alphastreet?sslmode=require" >"$TMP/url"
  if aws secretsmanager describe-secret --secret-id "$name" >/dev/null 2>&1; then
    aws secretsmanager put-secret-value --secret-id "$name" --secret-string "file://$TMP/url" >/dev/null
  else
    aws secretsmanager create-secret --name "$name" --secret-string "file://$TMP/url" --description "$3" >/dev/null
  fi
  echo "DATABASE_URL stored in $name: $(aws secretsmanager describe-secret --secret-id "$name" --query ARN --output text)"
}

echo "roles.sql applied."
store_url svc_app "$SVC_APP_PASSWORD" "alphaStreet-api DATABASE_URL (svc_app on the alphastreet DB)"
store_url svc_sim "$SVC_SIM_PASSWORD" "alphastreet-simulation SIM_DATABASE_URL (svc_sim on the alphastreet DB)"
