#Script to add local ip into aws security group

myip="$( curl -s https://api.ipify.org )"
aws ec2 authorize-security-group-ingress --protocol tcp --port 5432 --cidr $myip/32 --group-id  sg-935a01ea  
