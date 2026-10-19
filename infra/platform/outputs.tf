output "endpoint" {
  value = aws_db_instance.platform.address
}


# alphaStreet-api tfvars: shared_db_sg_id
output "rds_sg_id" {
  value = aws_security_group.rds.id
}


# For running roles.sql as master; services never use it.
output "master_user_secret_arn" {
  value = aws_db_instance.platform.master_user_secret[0].secret_arn
}
