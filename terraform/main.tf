terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

# Network for all services
resource "docker_network" "finance_network" {
  name   = "finance-data-network"
  driver = "bridge"
}

# PostgreSQL database
resource "docker_container" "postgres" {
  name  = "finance-postgres"
  image = "postgres:16"
  restart = "unless-stopped"
  
  networks_advanced {
    name = docker_network.finance_network.name
  }
  
  env = [
    "POSTGRES_USER=airflow",
    "POSTGRES_PASSWORD=airflow",
    "POSTGRES_DB=airflow_metadata"
  ]
  
  ports {
    internal = 5432
    external = 5433
  }
}

# Redis for Airflow
resource "docker_container" "redis" {
  name  = "finance-redis"
  image = "redis:7.2-bookworm"
  restart = "unless-stopped"
  
  networks_advanced {
    name = docker_network.finance_network.name
  }
  
  ports {
    internal = 6379
    external = 6380
  }
}
