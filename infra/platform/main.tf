# The shared platform database (alphaStreet-api docs/PLATFORM_ARCHITECTURE.md §4): one instance,
# one database `alphastreet`, one schema per service. Own state, separate from staging/, so an OCN
# apply never touches it. alphaStreet-api moves in first (schema app); OCN services follow one
# database at a time. Roles and schemas are SQL (roles.sql), not Terraform: RDS is private.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # prod: terraform init -backend-config="key=platform/prod/terraform.tfstate"
  backend "s3" {
    bucket         = "ocn-terraform-state"
    key            = "platform/staging/terraform.tfstate"
    region         = "eu-north-1"
    dynamodb_table = "ocn-terraform-lock"
    encrypt        = true
  }
}


provider "aws" {
  region = var.aws_region
}


# --- Network: OCN's VPC, looked up by name ---

data "aws_vpc" "ocn" {
  tags = {
    Name = "${var.env}-ocn-vpc"
  }
}


data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.ocn.id]
  }
  filter {
    name   = "tag:Name"
    values = ["${var.env}-private-*"]
  }
}


resource "aws_db_subnet_group" "platform" {
  name       = "${var.env}-platform-rds-subnet-group"
  subnet_ids = data.aws_subnets.private.ids
}


# No rules inline: each client is one aws_security_group_rule below, so adding a service is a
# one-line change to client_sg_ids.
resource "aws_security_group" "rds" {
  name        = "${var.env}-platform-rds"
  description = "Shared platform Postgres: 5432 from client service SGs only"
  vpc_id      = data.aws_vpc.ocn.id
}


resource "aws_security_group_rule" "rds_from_client" {
  for_each                 = var.client_sg_ids
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = each.value
  description              = each.key
}


# --- Postgres ---

# force_ssl is already RDS's pg16 default; pinned so it can't drift. RDS stores it as
# pending-reboot, so say so here or every plan shows a diff.
resource "aws_db_parameter_group" "platform" {
  name   = "${var.env}-platform-postgres16"
  family = "postgres16"

  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }

  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "pending-reboot"
  }
}


resource "aws_db_instance" "platform" {
  identifier            = "${var.env}-platform-postgres"
  engine                = "postgres"
  engine_version        = "16"
  instance_class        = var.instance_class
  allocated_storage     = 20
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true
  parameter_group_name  = aws_db_parameter_group.platform.name

  db_name  = "alphastreet"
  username = "master"
  # RDS generates and rotates the password in its own secret: never in tfvars or state.
  # Admin only (roles.sql); services connect as their svc_* role.
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.platform.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = var.multi_az

  # Holds user state (app.users, watchlists): PITR backups and a final snapshot.
  backup_retention_period   = var.backup_retention_days
  copy_tags_to_snapshot     = true
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "${var.env}-platform-postgres-final"

  performance_insights_enabled          = true
  performance_insights_retention_period = 7 # free tier

  # staging was created as `platform` and renamed in SQL (ALTER DATABASE ... RENAME). RDS keeps
  # reporting the initial name, and db_name forces replacement, so never diff on it.
  lifecycle {
    ignore_changes = [db_name]
  }
}
