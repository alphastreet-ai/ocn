# infra

Terraform infrastructure for the OCN platform. Provisions VPC, RDS, ECS Fargate cluster, ALB, security groups, IAM, and CloudWatch across environments.

## Stack

- **Terraform** - infrastructure as code
- **AWS ECS Fargate** - container orchestration
- **AWS RDS PostgreSQL 16** - shared relational database
- **AWS Secrets Manager** - runtime secrets injection
- **AWS S3 + DynamoDB** - remote state and locking

## Prerequisites

- Terraform >= 1.5
- AWS CLI configured with credentials for the target account
- Bootstrap applied (see below)

## First-time Setup

Run once per AWS account to create the remote state backend:

```bash
cd infra/bootstrap
terraform init
terraform apply
```

## Applying an Environment

```bash
cd infra/staging
terraform init
terraform apply -var-file=terraform.tfvars
```

`terraform.tfvars` is not committed. Create it from the required variables in `variables.tf`:

```hcl
db_master_password = "..."
ecr_registry       = "<account_id>.dkr.ecr.eu-north-1.amazonaws.com"
aws_account_id     = "<account_id>"
qdrant_host        = "<qdrant_host>"
```

## Secrets Manager

All application secrets must be populated in AWS Secrets Manager before deploying. ECS will fail to start any task whose `valueFrom` ARN cannot be resolved.

Expected secrets per service (path pattern: `ocn/{env}/{service}`):

| Service | Keys |
|---------|------|
| `auth-service` | `POSTGRES_PASSWORD`, `ADMIN_API_KEY` |
| `news-retrieval` | `POSTGRES_PASSWORD`, `OPENROUTER_API_KEY` |
| `signal-detection` | `POSTGRES_PASSWORD`, `OPENROUTER_API_KEY`, `QDRANT_API_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_PUBLIC_KEY` |

## Building and pushing images

Use the Makefile at the repo root - it handles the `linux/amd64` platform flag needed for ECS Fargate when building on Apple Silicon or other non-amd64 hosts.

```bash
make push-all        # build + push all three services
make push-auth       # build + push auth-service only
```

See the [root README](../README.md) for the full target list.

## CI/CD

GitHub Actions assumes the `ocn-github-actions` IAM role via OIDC (no long-lived credentials). The role has permission to push to ECR and update ECS services. It is scoped to pushes on `refs/heads/main` in `alphastreet-ai/ocn`.

## Structure

See [STRUCTURE.md](STRUCTURE.md) for a full file-by-file breakdown.
