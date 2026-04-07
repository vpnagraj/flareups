# flareups

## Overview
This repository includes a containerized data pipeline used to ingest, store, and visualize data from the [Cloudflare Radar API](https://developers.cloudflare.com/api/resources/radar/).

The pipeline is deployed in a (currently) private container image on GHCR. The image is used in a pod running on K3S in an AWS EC2 instance. The app writes data to other AWS resources (S3 and DynamoDB).

The output of the pipeline is a CSV file with relative L7 attack traffic data aggregated by hour, as well as a line plot (saved as a PNG file) visualizing these metrics. The output files are updated each time the job runs (i.e., hourly) and are written to S3.

## Repository structure

- `Dockerfile`: Dockerfile for the flareups imageK3S
- `cf_attacks.py`: Python app to pull data from the API, aggregate, load/fetch from DynamoDB, create visualization, sync plot to S3
- `requirements.txt`: Python requirements for the flareups app
- `aws/`: CloudFormation template for all AWS resources
- `k8s/`: Spec files for Kubernetes Job (for testing/debugging) and CronJob (for running at scale) resoures; also includes a script to create Secrets

## Launching the stack

**NOTE: All commands should be run from the repository root.**

First build the image and push to GHCR (assumes the user is logged into GHCR and GitHub PAT has been passed):

```bash
## build
docker build -t ghcr.io/vpnagraj/flareups:latest .

## push
docker push ghcr.io/vpnagraj/flareups:latest .
```

Next, create the AWS resources using CloudFormation. The command requires that an actual key name is passed:

```bash
aws cloudformation create-stack \
  --stack-name k3s-dev \
  --template-body file://aws/k3s-stack.yaml \
  --parameters \
    ParameterKey=KeyPairName,ParameterValue={actual-keypair-name} \
    ParameterKey=AllowedSSHCidr,ParameterValue="$(curl -s ifconfig.me)/32" \
  --capabilities CAPABILITY_NAMED_IAM
```

The CloudFormation template should create an EC2 instance, S3 buckets (one for a static site to house the plots generated; another for moving spec files, etc. into the instance), a DynamoDB table, and IAM roles / policies needed.

To view the outputs from CloudFormation:

```bash
aws cloudformation describe-stacks \
  --stack-name k3s-dev \
  --query 'Stacks[0].Outputs'
```

One of the outputs should be an `ssh` command to connect to the instance.

Connect to the instance and confirm K3S was installed successfully in the EC2 cloud init script:

```bash
kubectl get pods
```

Next, move the contents of the `k8s/` directory into the S3 bucket created for data transfer. 

Once complete, return to the running instance and use the `aws s3 cp` method to pull in the spec files and bash script to interactively generate Secrets. Run the script and generate resources in the instance:

```bash
## create secrets
bash create-secrets.sh

## create job to test
kubectl apply -f job.yaml

## check on the job
kubectl logs -f job/cf-attacks-fetch
kubectl get jobs

## provided that all looks good schedule the cronjob
kubectl apply -f job.yaml
kubectl get cronjobs
```

Return to the DynamoDB and/or S3 interface to watch data populate.