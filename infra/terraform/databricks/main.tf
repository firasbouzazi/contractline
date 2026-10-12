# Unity Catalog layout for contractline (ADR-0019): one catalog per environment
# and layer, a harness schema with volumes for checkpoints, groups per domain,
# and grants that keep raw personal data (bronze, quarantine) to PII readers.

terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }
}

provider "databricks" {}

variable "environments" {
  type    = list(string)
  default = ["dev", "staging", "prod"]
}

variable "domains" {
  description = "Keep in sync with contractline.yaml."
  type        = list(string)
  default     = ["sales", "finance", "marketing", "logistics", "hr"]
}

variable "deployer" {
  description = "Service principal (application id) that CD deploys and runs jobs as."
  type        = string
}

locals {
  layers     = ["bronze", "quarantine", "silver", "gold", "harness"]
  raw_layers = ["bronze", "quarantine"]
  catalogs = {
    for pair in setproduct(var.environments, local.layers) :
    "${pair[0]}_${pair[1]}" => { env = pair[0], layer = pair[1] }
  }
  domain_groups = merge(
    { for d in var.domains : "${d}_readers" => d },
    { for d in var.domains : "${d}_pii_readers" => d },
  )
}

resource "databricks_catalog" "layer" {
  for_each = local.catalogs
  name     = each.key
  comment  = "contractline ${each.value.layer} layer, ${each.value.env}"
  properties = {
    "contractline.env"   = each.value.env
    "contractline.layer" = each.value.layer
  }
}

resource "databricks_schema" "harness" {
  for_each     = toset(var.environments)
  catalog_name = databricks_catalog.layer["${each.key}_harness"].name
  name         = "contractline"
  comment      = "Run records, erasure and purge audit, checkpoints"
}

resource "databricks_volume" "checkpoints" {
  for_each     = toset(var.environments)
  catalog_name = databricks_catalog.layer["${each.key}_harness"].name
  schema_name  = databricks_schema.harness[each.key].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
}

resource "databricks_volume" "autoloader" {
  for_each     = toset(var.environments)
  catalog_name = databricks_catalog.layer["${each.key}_harness"].name
  schema_name  = databricks_schema.harness[each.key].name
  name         = "autoloader"
  volume_type  = "MANAGED"
}

resource "databricks_group" "domain" {
  for_each     = local.domain_groups
  display_name = each.key
}

# Everyone in a domain group may browse every catalog; table-level SELECT is
# granted per table by `contractline governance` (raw layers: PII readers only).
resource "databricks_grants" "catalog" {
  for_each = local.catalogs
  catalog  = databricks_catalog.layer[each.key].name

  grant {
    principal  = var.deployer
    privileges = ["ALL_PRIVILEGES"]
  }

  dynamic "grant" {
    for_each = contains(local.raw_layers, each.value.layer) ? [for d in var.domains : "${d}_pii_readers"] : keys(local.domain_groups)
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG", "USE_SCHEMA"]
    }
  }
}

resource "databricks_secret_scope" "contractline" {
  name = "contractline"
}

resource "databricks_secret_acl" "deployer" {
  scope      = databricks_secret_scope.contractline.name
  principal  = var.deployer
  permission = "READ"
}

output "catalogs" {
  value = keys(local.catalogs)
}
