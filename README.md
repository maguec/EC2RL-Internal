EC2RL-Internal
==============

EC2RL are set of scripts which helps you migrate prospects form AWS ElastiCache to Redis Enterprise.
The scripts extracts the current usage in ElastiCache, calculates the dataset and throughput and finally use the planner to report your expected cluster including infrastructure and estimated shard usage.

## How it works
There are 3 scripts in order to achieve these goals:

### pullElasticCacheStats
This pullElasticCacheStats script connects to your AWS account using boto3 (AWS API), and pulls out your current ElastiCache usage.
The script pulls the stats from ElastiCache, CloudWatch and Cost Estimator API's for a a specified region.
First the ElastiCache clusters information is extracted such as number of clusters and instance types.
Then additional information is extracted from CloudWatch, such as the operations types and throughput, network utilization that are needed in order to plan a well fitted Redis Enterprise Cluster.

You can see a sample out put sampleStats.csv in the outputs folder.

### calcElasticCacheStats
This script calculates the dataset and throughput based on the information gathered using pullElasticCacheStats.
The script does the following:

1. Takes the highest memory size from the nodes within a cluster and multiplies by the number of master shards
2. Sums the operations from all the nodes within the cluster and multiplies by 10 as a buffer
3. Checks if the cluster is highly available, and verifies that it has more than 2 replicas (Limited HA? column)
4. Sums the network utilization and packets in+out

You can see a sample out put sampleCalculatedStats.xlsx in the outputs folder.

### planRedisCluster
This script sends the information in the format of the calcElasticCacheStats output to the planner in order to get the hardware and number of shards to handle the cluster requirements.
The output of the planner is the number of shards and the hardware that is needed including convertible instances in the case we need an estimation for a hosted cluster. 

You can see a sample out put samplePlannedCluster.xlsx in the outputs folder.

## Getting Started

```
# Clone:
git clone https://github.com/Redislabs-Solution-Architects/EC2RL-Internal

# Prepare virtualenv:
cd EC2RL-Internal
mkdir .env
virtualenv .env

# Activate virtualenv
. .env/bin/activate

# Install boto3
pip install boto3
pip install pandas
pip install xlsxwriter
pip install requests
pip install xlrd

# When finished
deactivate
```

### pullElasticCacheStats
In order to run the script just pass the path to the JSON config file

```
python pullElasticCacheStats.py pullStatsConfig.json
```

The pullStatsConfig.json should contain the following information
```
{
    "accessKey": "Your AWS Access Key",
    "secretKey": "Your AWS Secret Key",
    "region": "AWS region for example us-east-2"
}
```

### calcElasticCacheStats
In order to run the script just pass the path to the csv with the raw data that was collected with pullElasticCacheStats

```
python calcElasticCacheStats.py fileToBeProcessed.csv
```

### planRedisCluster
In order to run the script just pass the path to the JSON config file

```
python planRedisCluster.py planClusterConfig.json
```

The planClusterConfig.json should contain the following information
```
{
    "plannerURL": "https://api-beta1-qa.redislabs.com/beta1",
    "x-api-key": "planner x-api-key",
    "x-api-secret-key": "planner x-api-secret-key",
    "inputFile": "clusterInformation.xlsx",
    "aws_access_key_id": "Your AWS Access Key",
    "aws_secret_access_key": "Your AWS Secret Key",
    "pricingRegion": "Should always be us-east-1"
}
```
### TBD
* Add GCP to planRedisCluster
* For some regions convertible price is missing
