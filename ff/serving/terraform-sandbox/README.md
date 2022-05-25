## Sandbox instructions

1. Open main.tf
2. Adjust number of sandboxes if necessary. To add a new sandbox, add an additional module block. 
To remove, delete the module block.

```
module "<NAME>"  {
   source = "./sandbox"
   sandbox_name = "<NAME>"
}
```
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply`` then type ``yes`` when prompted

## Adding EKS Context To Kubectl
