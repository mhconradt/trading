terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.43.0"
    }
    random = {
      source = "hashicorp/random"
      version = "~> 3.0.1"
    }
    kubernetes = {
      source = "hashicorp/kubernetes"
      version = "2.3.2"
    }
  }
}

provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region = var.aws_region
}

data "aws_eks_cluster" "cluster" {
  name = module.k8s-cluster.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.k8s-cluster.cluster_id
}

provider "kubernetes" {
  host = data.aws_eks_cluster.cluster.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority.0.data)
  token = data.aws_eks_cluster_auth.cluster.token
  version = "~> 2.3.2"
}

###############################################################################
# DATA SOURCES
###############################################################################

data "aws_availability_zones" "available" {
  state = "available"
}

###############################################################################
# VPC
###############################################################################

resource "aws_vpc" "vpc" {
  cidr_block = local.network_address_space

  tags = merge(local.common_tags, {
    Name = "${local.env_name}-vpc"
  })
}

###############################################################################
# SUBNETS
###############################################################################

resource "aws_subnet" "public" {
  count = local.subnet_count

  map_public_ip_on_launch = true

  cidr_block = cidrsubnet(local.public_address_space, 4, count.index)
  vpc_id = aws_vpc.vpc.id
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(local.common_tags, {
    Name = "${local.env_name}-public-subnet${count.index}",
    SubnetType = "public",
    "kubernetes.io/cluster/${local.env_name}-k8s-cluster" = "shared",
    "kubernetes.io/role/elb" = 1
  })
}

###############################################################################
# ROUTE TABLES
###############################################################################


resource "aws_route_table" "public" {
  vpc_id = aws_vpc.vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.public.id
  }

  tags = merge(local.common_tags, {
    Name = "${local.env_name}-public-rtb"
  })
}

resource "aws_internet_gateway" "public" {
  vpc_id = aws_vpc.vpc.id
}

###############################################################################
# NETWORK ACCESS CONTROL LISTS
###############################################################################

resource "aws_network_acl" "public" {
  vpc_id = aws_vpc.vpc.id
  subnet_ids = aws_subnet.public[*].id

  ingress {
    action = "ALLOW"
    rule_no = 100
    protocol = -1
    to_port = 0
    from_port = 0
    cidr_block = "0.0.0.0/0"
  }

  egress {
    action = "ALLOW"
    rule_no = 100
    protocol = -1
    to_port = 0
    from_port = 0
    cidr_block = "0.0.0.0/0"
  }

  tags = merge(local.common_tags, {
    Name = "${local.env_name}-public-nacl"
  })
}

###############################################################################
# ROUTE TABLE ASSOCIATIONS
###############################################################################

resource "aws_route_table_association" "public" {
  count = local.subnet_count

  route_table_id = aws_route_table.public.id
  subnet_id = aws_subnet.public[count.index].id
}

###############################################################################
# KUBERNETES
###############################################################################

module "k8s-cluster" {
  source = "terraform-aws-modules/eks/aws"
  version = "17.1.0"

  cluster_name = format("%s-cluster", local.env_name)
  cluster_version = "1.19"
  subnets = slice(aws_subnet.public[*].id, 0, 3)
  vpc_id = aws_vpc.vpc.id

  create_eks = true

  cluster_log_retention_in_days = 0

  workers_group_defaults = {
    root_volume_type = "gp2"
  }

  worker_groups = [
    {
      name = "default"
      instance_type = "m5.large"
      placement_tenancy = "default"
      volume_type = "gp2"
      subnets = slice(aws_subnet.public[*].id, 0, 3)
    }
  ]

  # Max kubectl access
  map_users = [
    {
      userarn = "arn:aws:iam::565925249289:user/max"
      username = "max"
      groups = ["system:masters"]
    }]

  tags = merge(local.common_tags, {
    Name = format("%s-k8s-cluster", local.env_name)
  })

  write_kubeconfig = false
}