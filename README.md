# set up environment

## you will need python 3.10 - use pyenv perhaps to be able to install various pythons

## virtual environment
```
$ python3 --version
$ python3.10 -m venv .venv-3.10
$ source .venv-3.10/bin/activate
(venv) $
```

## install requirements
`pip install -r requirements.txt`

## config things
#### copy creds.copy.json to creds.json and replace your username and password (w/ passphrase from key gen process below)

# connect to snowflake

### there are two ways to connect - keys (prefered) vs. browser auth

# public private key pai set up

#### gen private key encrypted
`mkdir keys`
`cd keys`
`openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.pem`
-rw-------   1 kcbigring  staff  1886 Apr  4 09:19 rsa_key.pem
-rw-r--r--   1 kcbigring  staff   451 Apr  4 09:20 rsa_key_kcbigring.pub

#### gen public key
`openssl rsa -in rsa_key.pem -pubout -out rsa_key_kcbigring.pub` (replace kcbigring w/ your github handle name)

#### assign the public key to the first slot on the snowflake user
#### first you need to connect to snowflake or use the classic version in the web
#### USE YOUR ACCOUNT HERE
`snowsql -a ayb56057 -u kcbigring`

#### now set your public key in snowflake
`ALTER USER KCBIGRING SET RSA_PUBLIC_KEY = 'MIIBIjANB…';`

ALTER USER KCBIGRING SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArVlea7ESZ/oYAgMvkzQJ
lSltDi9FGmI9TNofExIeTwVtIxxL7X+cDWCJNAY5nNTZdzgB7IZRHKfY/OE6b2j6
vHQi+bzHeXDLuim3ur4YuFSj6EAcb94oCUlSeyDGbmgXjVRKeQCVN0SF9ZNCF2j5
65dA+Ty/TkThqw3/PgPUFevStydahJsP22RdqwTM8XJvTlSkzy0vgb2D6IGu6wSv
sx4MlL6/XrbprJbjKdvFc7DD2PCpQQpGRbW8rvyf/NzIgVajhmut9PuPakejQqqq
DjMo/8QmjlWnZk+DgQ4b9cXwMXrxkR1eJrBnGsT0u5fSomjK8ZfbXIJRuFB19pmo
AwIDAQAB';

ALTER USER DUSTINH SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuKvyV4X5RVSFKYF4/yMr
RZ1v6qxPpjegwa+OjBsk+Ug2qsRUybwO49osxei1JpnvVq+BzEFxfkilqrNEog4s
WxLYhClcJ3GLPwY77ehX4mjiFyGcPXZjELXFhtf3tN83IyzI6bgbaQ6grMVMwA+j
Qch02zkhQCPDLM1w0/eyO+EC2WSYL4tBjN6TIHhbAIQWE2Q/yAt+uvV4fr7bkoFt
izaY6xoP9V2YYpv9q3yVkIn3p0cBr9wMaMKzjk2e13KR+WU7jRh7FT3K7S1iMkRk
iQ/vOBkKgctE5wISSbmhf+52EMCrKREzQesl6H2Bcrm4AdJPrzhhuwRbUWJkTJLD
OQIDAQAB';

ALTER USER MATTHIAS SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7lddXOq/COhZPU7r9f7l
ScfzU5ISDUzvqUA4rF6H8BJq0ncHXCVEV7WxyMlUSIotvOJl1AmeC4+EL7a/3TEt
gGjA0Ur7XXx6oalry0PBNZRwtFF6yiYbsCe6koBPkXIYjYFPR75t4Ch0xJp3E+gq
9ed6SYC04yU1aTuVJfqUE2V6X1gLhItLjn55aZIX+CnJRcicpUVUgOjappbg/umw
MEalyL5p9R1TUDfIG/M+6gtI4buWmb5s69eCHWj/lx3fveJ4AkwrOB+ScTcybQ1I
ZTtYgMcZs8tVxkN4pBbAyJt6/jt4nrbB+B2/4PCybmHExZwgYrioBGzFl6GOgWMb
OwIDAQAB';


#### NOT NEEDED but to unseat a key
#### (DO NOT NOT NOT do this)
`ALTER USER XXXXX UNSET RSA_PUBLIC_KEY;`

#### Test the connection
`snowsql -a <account_identifier> -u <user> --private-key-path <path>/rsa_key.pem` -- will be prompted for passphrase
`snowsql -a ayb56057 -u kcbigring --private-key-path ./keys/rsa_key.pem`

#### can also auth w/ browser or password
`snowsql -a ayb56057 -u kcabigring`
`snowsql -a ayb56057 -u kcabigring --authenticator=externalbrowser`

# install prefect
#### ALREADY INSTALLED WHEN USING REQUIREMENTS.TXT
NOTE NEEDED >> `pip install -U prefect`

# authenticate
`prefect cloud login`

# NOTE!!!!
the snowflake-connector-python does not install "pyarrow" which you need to play with pandas.
either you could install and Import Pyarrow or
do :
pip install "snowflake-connector-python[pandas]"
---- for the LG deployment updated the EXTRA_PIP_PACKAGES until we have a docker image that pip installs from requirements.txt

#################### BEGIN MANAGED WORKERS ####################
# MANAGED PREFECT WORKER POOL

## create a prefect managed pool
`prefect work-pool create legacy-managed-pool --type prefect:managed`

## create the depoloyment
`python3 prefect_deploy.py`

## run the deployment either from UI or:
`prefect deployment run 'get-repo-info/legacy-managed-pool-deployment'`

#################### END MANAGED WORKERS ####################

YOU CAN STOP HERE AS THE REST IS WIP - TRYING TO GET ECS WORKING AND PAST THE DOCKER ISSUES
I THINK MANAGED WORKERS WILL WORK FOR US AND IS SOOOO SIMPLE

#################### BEGIN ECS WORKER POOLS ####################

#### THIS IS WIP AND CURRENTLY NOT WORKING - STOP HERE ####

# step 1: set up an ECS worker pool from prefect.io
`prefect work-pool create --type ecs leacy-ecs-pool`

--- to start the pool
`prefect worker start --pool leacy-ecs-pool`

# step 2: start a prefect worker in ECS cluster

#### create a trust policy (see ecs-trust-policy.json)

#### create the IAM role 
`aws iam create-role \
--role-name ecsTaskExecutionRole \
--assume-role-policy-document file://ecs-trust-policy.json`

#### attach the policy to the role (require permissions to pull images from ECR and publish logs to CloudWatch)
`aws iam attach-role-policy \
--role-name ecsTaskExecutionRole \
--policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy`

# step 3: create an ECS worker service

## set up ECS cluster using AWS web console

## launch and ECS service to host the worker - see task-definition.json
#### `prefect config view` - get prefect api url
#### prefect api key - se prefect.io
#### <ecs-task-role-arn> - `aws iam get-role --role-name ecsTaskExecutionRole --query 'Role.[RoleName, Arn]' --output text`

## regiser the task definition
`aws ecs register-task-definition --cli-input-json file://task-definition.json`

## create an ECS fargate service to host your worker
`aws ecs create-service \
    --service-name prefect-worker-service \
    --cluster  legacy-ecs-cluster \
    --task-definition arn:aws:ecs:us-west-2:883952409056:task-definition/prefect-worker-task:3 \
    --launch-type FARGATE \
    --desired-count 1 \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-0b48a921, subnet-d209618f, subnet-e6a71cac, subnet-3259c74a],securityGroups=[sg-d98a91d1],assignPublicIp='ENABLED'}"`

#### 
`aws ec2 describe-subnets --filters "Name=vpc-id,Values=vpc-4f929f37"`

`aws ecr create-repository \
--repository-name legacy-ecr-repo \
--region us-west-2`

{
    "repository": {
        "repositoryArn": "arn:aws:ecr:us-west-2:883952409056:repository/legacy-ecr-repo",
        "registryId": "883952409056",
        "repositoryName": "legacy-ecr-repo",
        "repositoryUri": "883952409056.dkr.ecr.us-west-2.amazonaws.com/legacy-ecr-repo",
        "createdAt": "2024-05-21T00:05:15.862000-06:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}

`prefect deploy my_flow.py:legacy-ecs-deployment`

#################### END ECS WORKER POOLS ####################

