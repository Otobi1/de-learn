### Basics 

##### Terraform 

**Terraform** is used to define the resources and configuration for a cloud service. It is **Infrastructure as Code**.

The primary advantage is for the management of cloud service in a structured, source controlled way, which also enables effective governance.

In this case, I've used Terraform to create a Google Cloud Storage bucket and BiqQuery Dataset. Here is an overview of the blocks in the main.tf file 

    - Terraform Provider Block 

    - [Google Provider Config](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket)

    - Cloud Storage Bucket resource definition, including name, location,  storage class and bucket level access

    - BigQuery Dataset specifying the dataset name and location

Variables.tf contains the parameters for each of the different fields to improve code consistency and readability.

Commands include 

    `terraform init`: Initializes a working directory, downloads necessary provider plugins, and prepares the environment.

    `terraform plan`: Creates an execution plan, showing the changes Terraform will make to your infrastructure without applying them.

    `terraform apply` : Applies the planned changes to your infrastructure, creating or modifying resources as defined in the configuration.

    `terraform destroy`: Destroys the infrastructure managed by Terraform, removing all resources defined in the configuration.

    `terraform fmt`:Automatically formats Terraform code according to standard style conventions.

    `terraform validate`: Validates the Terraform configuration for syntax and internal consistency errors.

    `terraform show`: Displays the state or output of the Terraform-managed infrastructure.

    `terraform output`: Extracts and displays the values of outputs from the last applied configuration.


##### Docker 
- what is ? 
- how to ? 