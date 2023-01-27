# Education Data Platform (EDP)'s concepts

Modernizing data state in an educational institution is always a big challenge as it does require changes in many areas across the entire organization. Obviously, technology is a critical part of the change and as such, has to change as well.

Google Cloud offers leading services on top of its highly scalable data platform, (aka, [BigQuery](https://cloud.google.com/bigquery), to make that task easier, helping educational institutions to become data-driven faster and so adhering to digital education.

**Education Data Platform** is a combination of [reference architecture](edp-architecture.md) and scripts that deploy it in Google Cloud for creating a **modern data platform** that brings together Data Lake, Data Warehouse, Common Data Models for Education and specialized AI models.

It was designed taking into consideration Google Cloud's best practices for data and governance combined with all the main challenges educational institutions face every day with data to streamline that process.

Below you can get familiar with some of the concepts we bring by introducing EDP.

## Landing Zone for governance

This module implements an opinionated data platform that creates and set up projects and related resources that compose an end-to-end data repository on top of BigQuery and underlying services.

The code is intentionally simple, as it's intended to provide a generic initial setup and then allow easy customizations to complete the implementation of the intended design.

## Connectors

The Cloud Composer will be used as an orchestration solution for data ingestion by connectors (through pipelines) from popular educational solutions' databases, like Moodle and others. Connectors for Restful Open APIs and other processes like auxiliary Directed Acyclic Graph (DAG).

## Education-ready Machine Learning models (coming soon)

The future of Education leans towards Artificial Intelligence (AI). That's why EDP brings to life a set of ready-to-go machine learning models on top of the data sitting on EDP that solves common problems we've seen across the globe in Education.

More coming up soon.

## Common Data Model (coming soon)

EDP will soon bring to life, an alive and always evolving Common Data Model (CDM) specifically designed for Education, which simplifies the process of building analytical analysis and dashboard and the construction of education-focused machine learning models.

More coming up soon. If you want to help us mature EDP's CDM, please use the "Issues" session in this repo to submit your collaboration through Pull Request.

## Python-based API

The API is intended to consume other Restful Open APIs and generate files (JSON or CSV) inside a bucket within the data repository.

The API was built using a Cloud Function and its usage is very flexible, allowing the parallel consumption of several endpoints in parallel.

## Modules

The suite of modules in this repository is designed for rapid composition and reuse, and to be reasonably simple and readable so that they can be forked out and changed where the usage of third-party code and sources is not allowed.

* All modules do share a similar interface as they're meant to stay closer to the underlying provider's resources.
* It does support Identity Access Manager (IAM) for resource creation and update.
* Also, it does offer the option of creating multiple resources (where it makes sense) at once, freeing up potential side effects (like external commands).

The current list of modules supports most of the core foundational and networking components used to design end-to-end infrastructure, with more modules in active development for specialized compute, security, and data scenarios.

Currently available modules:

- **foundational** - [billing budget](terraform-modules/billing-budget), [Cloud Identity group](terraform-modules/cloud-identity-group/), [folder](terraform-modules/folder), [service accounts](terraform-modules/iam-service-account), [logging bucket](terraform-modules/logging-bucket), [organization](terraform-modules/organization), [project](terraform-modules/project), [projects-data-source](terraform-modules/projects-data-source)
- **networking** - [DNS](terraform-modules/dns), [Cloud Endpoints](terraform-modules/endpoints), [address reservation](terraform-modules/net-address), [NAT](terraform-modules/net-cloudnat), [Global Load Balancer (classic)](terraform-modules/net-glb/), [L4 ILB](terraform-modules/net-ilb), [L7 ILB](terraform-modules/net-ilb-l7), [VPC](terraform-modules/net-vpc), [VPC firewall](terraform-modules/net-vpc-firewall), [VPC peering](terraform-modules/net-vpc-peering), [VPN dynamic](terraform-modules/net-vpn-dynamic), [HA VPN](terraform-modules/net-vpn-ha), [VPN static](terraform-modules/net-vpn-static), [Service Directory](terraform-modules/service-directory)
- **compute** - [VM/VM group](terraform-modules/compute-vm), [MIG](terraform-modules/compute-mig), [COS container](terraform-modules/cloud-config-container/cos-generic-metadata/) (coredns, mysql, onprem, squid), [GKE cluster](terraform-modules/gke-cluster), [GKE hub](terraform-modules/gke-hub), [GKE nodepool](terraform-modules/gke-nodepool)
- **data** - [BigQuery dataset](terraform-modules/bigquery-dataset), [Bigtable instance](terraform-modules/bigtable-instance), [Cloud SQL instance](terraform-modules/cloudsql-instance), [Data Catalog Policy Tag](terraform-modules/data-catalog-policy-tag), [Datafusion](terraform-modules/datafusion), [GCS](terraform-modules/gcs), [Pub/Sub](terraform-modules/pubsub)
- **development** - [API Gateway](terraform-modules/api-gateway), [Apigee](terraform-modules/apigee), [Artifact Registry](terraform-modules/artifact-registry), [Container Registry](terraform-modules/container-registry), [Cloud Source Repository](terraform-modules/source-repository)
- **security** - [Binauthz](terraform-modules/binauthz/), [KMS](terraform-modules/kms), [SecretManager](terraform-modules/secret-manager), [VPC Service Control](terraform-modules/vpc-sc)
- **serverless** - [Cloud Function](terraform-modules/cloud-function), [Cloud Run](terraform-modules/cloud-run)

For more information and usage examples see each module's README file.