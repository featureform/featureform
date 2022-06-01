## Terraform instructions

1. Open main.tf

```
module "<NAME>"  {
   source = "./cluster"
   name = "<NAME>"
}
```
3. Run ``terraform init``
4. Run ``terraform plan``
5. Run ``terraform apply`` then type ``yes`` when prompted

