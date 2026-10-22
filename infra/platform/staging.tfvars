# terraform plan -var-file=staging.tfvars  (no secrets here: the master password is RDS-managed)

client_sg_ids = {
  # staging-bastion-temp: roles.sql via SSM port forward (bootstrap-app.sh). Drop with the bastion.
  bastion = "sg-0d3f55f6a7b541cb0"
  # api, worker and migration tasks (api_sg_id output of alphaStreet-api/infra)
  alphastreet-api = "sg-0167ae88c0c934286"
  # nightly, migrate and pollers tasks (sim_sg_id output of alphastreet-simulation/infra): uncomment
  # once that root is applied, then rerun bootstrap-app.sh.
  # alphastreet-sim = "sg-..."
}
