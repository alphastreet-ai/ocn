variable "env" {
  type    = string
  default = "staging"
}


variable "aws_region" {
  type    = string
  default = "eu-north-1"
}


# micro while only alphaStreet-api lives here; size up as OCN services move in. prod: a
# non-burstable class (or alarm on CPUCreditBalance) and multi_az = true.
variable "instance_class" {
  type    = string
  default = "db.t4g.micro"
}


variable "multi_az" {
  type    = bool
  default = false
}


variable "backup_retention_days" {
  type    = number
  default = 7
}


# Service name => its ECS security group id, e.g. { "alphastreet-api" = "sg-..." }.
variable "client_sg_ids" {
  type    = map(string)
  default = {}
}
