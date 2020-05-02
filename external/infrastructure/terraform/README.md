## Create aws EC2 instances

### Requirements:

* tested on ubuntu 18.04 (should work also on mac)
* aws account
* terraform >= 0.12     

https://www.terraform.io/downloads.html

* run: `cp secrets.tf.example secrets.tf`
* setup your secrets (aws key, aws secret, your public key)
* setup instances numbers (broker, producer, consumer)

```bash
cd aws-ec2/
terraform init #(init aws provider, should be run one time at start)
terraform apply #(create resources)
terraform destroy #(destroy resources)
```
