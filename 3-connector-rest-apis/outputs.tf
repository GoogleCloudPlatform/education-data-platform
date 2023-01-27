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

output "function" {
  description = "The reserved global IP address."
  value       = module.function.function
}

output "service_account" {
  description = "Service account resource."
  value       = module.function.service_account
}

output "service_account_email" {
  description = "Service account email."
  value       = module.function.service_account_email
}

output "uri" {
  description = "Cloud function service uri."
  value       = module.function.uri
}