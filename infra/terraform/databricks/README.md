# Databricks infrastructure

Creates the Unity Catalog layout contractline expects: catalogs
`<env>_<layer>` for `bronze`, `quarantine`, `silver`, `gold` and `harness`,
volumes for streaming checkpoints and Auto Loader state, groups per domain
(`<domain>_readers`, `<domain>_pii_readers`), catalog grants, and the
`contractline` secret scope holding the masking keys (`pii_hash`, `pii_token`).

```bash
export DATABRICKS_HOST=... DATABRICKS_TOKEN=...   # or a CLI profile
terraform init
terraform apply -var deployer=<service-principal-application-id>
```

Then put the masking keys in the scope (never in the repository):

```bash
databricks secrets put-secret contractline pii_hash  --string-value "$(openssl rand -hex 32)"
databricks secrets put-secret contractline pii_token --string-value "$(openssl rand -hex 32)"
```
