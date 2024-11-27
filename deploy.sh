#!/bin/bash
set -e

ENV=${1:-dev}
if [[ ! "$ENV" =~ ^(dev|prod)$ ]]; then
    echo "Error: Environment must be 'dev' or 'prod'"
    exit 1
fi

echo "Deploying to $ENV environment..."

cd infrastructure

terraform init

terraform workspace select $ENV || terraform workspace new $ENV

terraform apply -var="environment=$ENV" -auto-approve

terraform output -json > terraform.output.$ENV.json

echo "Deployment to $ENV complete. Outputs saved in terraform.output.$ENV.json"

terraform workspace select default

cd ..

python scripts/setup.py --env $ENV