# Define the GCP project ID
variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

# Define the region
variable "region" {
  description = "The region where resources will be created"
  type        = string
}

# Define the zone for VM
variable "zone" {
  description = "The zone where the VM instance will be deployed"
  type        = string
}

# Define the VM name
variable "vm_name" {
  description = "The name of the VM instance"
  type        = string
}

# Define the machine type for the VM
variable "machine_type" {
  description = "Machine type for the compute instance"
  type        = string
}

# Define the SSH key file for accessing the VM
variable "ssh_key_file" {
  description = "Path to the SSH private key file"
  type        = string
}
