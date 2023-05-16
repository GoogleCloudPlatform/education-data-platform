/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module "project" {
  source = "../modules/project"
  billing_account = (var.project_create != null
    ? var.project_create.billing_account_id
    : null
  )
  parent = (var.project_create != null
    ? var.project_create.parent
    : null
  )
  name = var.project_id
  services = [
    "apigateway.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "compute.googleapis.com",
    "servicemanagement.googleapis.com",
    "servicecontrol.googleapis.com"
  ]
  project_create = var.project_create != null
}

module "sa" {
  source     = "../modules/iam-service-account"
  project_id = module.project.project_id
  name       = "sa-api"
  iam_project_roles = {
    (var.project_id) = [
      "roles/cloudfunctions.invoker",
      "roles/storage.objectCreator",
      "roles/storage.objectViewer",
    ]
  }
}


module "function" {
  source           = "../modules/cloud-function"
  project_id       = module.project.project_id
  name             = var.prefix
  bucket_name      = "bkt-${module.project.project_id}"
  region           = var.region
  ingress_settings = "ALLOW_ALL"
  bucket_config = {
    location                  = null
    lifecycle_delete_age_days = 1
  }
  bundle_config = {
    source_dir  = "${path.module}/gcp_api"
    output_path = "${path.module}/bundle.zip"
    excludes    = null
  }
  function_config = {
    entry_point = "main"
    instance_count   = var.instances
    memory      = 256
    runtime     = "python39"
    timeout     = 480 # Timeout in seconds, increase it if your CF timeouts and use v2 if > 9 minutes.
  }
  service_account = module.sa.email
  service_account_create = false
  iam = {
    "roles/cloudfunctions.invoker" = [
      module.sa.iam_email,
      "serviceAccount:${var.composer_iam_email}",
    ]
  }

  environment_variables = {
    BUCKET_ID = var.bucket_id
  }
}

