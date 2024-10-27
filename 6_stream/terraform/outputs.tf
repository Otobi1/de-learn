# Output the public IP address of the VM
output "vm_name" {
  description = "The name of the VM instance"
  value       = google_compute_instance.de-stream
}

output "vm_public_ip" {
  description = "The public IP address of the VM"
  value       = google_compute_instance.de-stream.network_interface[0].access_config[0].nat_ip
}