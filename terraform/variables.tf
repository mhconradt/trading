variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "aws_region" {}

locals {
  env_name = "prod"

  network_address_space = "10.42.0.0/16"
  subnet_count = 3

  common_tags = {
    Environment = local.env_name
  }

  private_address_space = cidrsubnet(local.network_address_space, 4, 0)
  public_address_space = cidrsubnet(local.network_address_space, 4, 3)
}