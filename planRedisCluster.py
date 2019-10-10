# input params
# Input File path

import pandas as pd
import json, math, time, requests, boto3
from requests.auth import HTTPDigestAuth
from pathlib import Path
from pkg_resources import resource_filename

# Search product filter
FLT = '[{{"Field": "tenancy", "Value": "shared", "Type": "TERM_MATCH"}},'\
      '{{"Field": "operatingSystem", "Value": "Linux", "Type": "TERM_MATCH"}},'\
      '{{"Field": "preInstalledSw", "Value": "NA", "Type": "TERM_MATCH"}},'\
      '{{"Field": "instanceType", "Value": "{t}", "Type": "TERM_MATCH"}},'\
      '{{"Field": "LeaseContractLength", "Value": "1yr", "Type": "TERM_MATCH"}},'\
      '{{"Field": "PurchaseOption", "Value": "All Upfront", "Type": "TERM_MATCH"}},'\
      '{{"Field": "location", "Value": "{r}", "Type": "TERM_MATCH"}}]'

      
# Translate region code to region name
def get_region_name(region_code):
    default_region = 'EU (Ireland)'
    endpoint_file = resource_filename('botocore', 'data/endpoints.json')
    try:
        with open(endpoint_file, 'r') as f:
            data = json.load(f)
        return data['partitions'][0]['regions'][region_code]['description']
    except IOError:
        return default_region

def getColumns():
    cols = ['Region', 'Type', 'Quantity', 'Quantity Measurement', 'Price Per Unit', 'Convertible Price/Unit Reserved', 'Price Period']
    return cols

def getMachinePrices(region, machine):
    session = boto3.Session(
        aws_access_key_id=inputParams['aws_access_key_id'], 
        aws_secret_access_key=inputParams['aws_secret_access_key'],
        region_name=inputParams['pricingRegion'])
    pr = session.client('pricing')
    f = FLT.format(r=get_region_name(region), t=machine)
    response = pr.get_products(ServiceCode='AmazonEC2', Filters=json.loads(f))
    response = json.loads(response['PriceList'][0])
    terms = response['terms']
    for term in terms['Reserved'].values():
        leaseContractLength = term['termAttributes']['LeaseContractLength']
        offeringClass = term['termAttributes']['OfferingClass']
        purchaseOption = term['termAttributes']['PurchaseOption']
        if offeringClass == 'convertible' and leaseContractLength == '1yr' and purchaseOption == 'All Upfront':
            for price in term["priceDimensions"].values():
                price = price['pricePerUnit']["USD"]
                if float(price) > 0:
                    return float(price)

def createSubscription(inputFile, isAZ):
    """Call the create subscription API according to the input file
    Args:
        inputFile (string): The file path that holds the information for creating a cluster
        isAz (string): Does the cluster need to support multi AZ ('true'/'false')
    Returns:
        taskId (string): The task id that holds the response for the create subscription
    """
    databases = []

    df = pd.read_excel(inputFile)
    region = df.iloc[0]['Region']

    req = {
        "name": "Generated DB",
        "dryRun": "true",
        "memoryStorage": "ram",
        "paymentMethodId": 8240,
        "cloudProviders": [
        {
            "cloudAccountId": 9838,
            "regions": [
            {
                "region": region,
                "multipleAvailabilityZones": isAZ,
                "networking": {
                "deploymentCIDR": "10.0.0.0/24"
                }
            }
            ]
        }
        ]
    }

    for (index, row) in df.iterrows():        
        clusterAPI = "false"
        if row['Cluster API?']:
            clusterAPI = "true"

        replication = "false"
        if row['HA?']:
            replication = "true"
        
        print(row['DB Name'])
        ops = round(math.ceil( row['Ops/Sec'] / 1000 ) * 1000)
        if row['Ops/Sec'] < 1000:
            ops = 1000
        
        mem = round( math.ceil( row['Memory Limit (GB)'] / 0.1 ) * 0.1, 1)
        if row['Memory Limit (GB)'] < 0.1:
            mem = 0.1

        databases.append(
            {
                "name": row['DB Name'],
                "throughputMeasurement": {
                "by": "operations-per-second",
                "value": ops
                },        
                "dataPersistence": "none",
                "replication": replication,
                "memoryLimitInGb": mem,
                "supportOSSClusterApi": clusterAPI                
            }
        )

    req['databases'] = databases
    res = s.post(url=rootUrl+'/subscriptions', json=req)
    if(res.ok):
        # Loading the response data into a dict variable
        # json.loads takes in only binary or string variables so using content to fetch binary content
        # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
        jData = json.loads(res.content)
        print("Called create subscription with task id: " + jData['taskId'])
        return jData['taskId']
    else:
        res.raise_for_status()
        return None


def writeResult(inputFile, outputFile, data):
    """Write the cluster costs to the output file
    Args:
        outputFile (string): The file path to write cluster costs
        data (json): The data that was returned from the get task status request
    Returns:
        Nothing
    """
    # write the result to the Cluster Plan tab
    writer = pd.ExcelWriter(outputFile, engine='xlsxwriter')
    outDF = pd.DataFrame(columns=getColumns())
    inputDF = pd.read_excel(inputFile)
    region = inputDF.iloc[0]['Region']
    convertiblePrice = ''

    index = 0
    for row in data['resource']['pricing']:
        price = row['pricePerUnit']
        pricePeriod = row['pricePeriod']
        if row['type'] == 'Shards':
            price = ''
            pricePeriod = ''

        elif row['type'] == 'EBS Volume':
            price = price * 744 *12
            pricePeriod = 'Year'
        
        else:
            price = price * 744 *12
            pricePeriod = 'Year'
            convertiblePrice = getMachinePrices(region, row['type'])

        outDF.loc[index] = [region, row['type'], row['quantity'], row['quantityMeasurement'], price, convertiblePrice, pricePeriod]
        index = index + 1
    outDF.to_excel(writer, 'Cluster Plan')

    # write the input data to the Input Data tab
    inputDF.to_excel(writer, 'Input Data')

    writer.save()



def processSubscriptionRequest(inputFile, outputFile, taskId):
    """Write the cluster costs to the output file
    Args:
        outputFile (string): The file path to write cluster costs
        taskId (string): The task id that was returned from the create subscription call
    Returns:
        Nothing
    """
    taskPending = True
    while taskPending:
        res = s.get(rootUrl+ '/tasks/' + taskId)
        if(res.ok):

            # Loading the response data into a dict variable
            # json.loads takes in only binary or string variables so using content to fetch binary content
            # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
            jData = json.loads(res.content)
            print("Waiting for subscription results")
            if jData['status'] == "processing-in-progress":
                time.sleep(10)
            if jData['status'] == "processing-completed":
                writeResult(inputFile, outputFile, jData['response'])
                taskPending = False
            if jData['status'] == "processing-error":
                print("Failed to create subscription: " + jData['response']['error']['description'])
                taskPending = False
        else:
            taskPending = False
            res.raise_for_status()

with open('planClusterConfig.json') as config_file:
    inputParams = json.load(config_file)

rootUrl = inputParams['plannerURL']
s = requests.Session()
s.headers.update(
    {
        'x-api-key': inputParams['x-api-key'],
        'x-api-secret-key': inputParams['x-api-secret-key'],
        'Accept': 'application/json'
    })

inputFile = inputParams['inputFile']
outputFile = Path(inputFile).stem + "-pricing" + ".xlsx"
isAZ='false'

taskId = createSubscription(inputFile, isAZ=isAZ)
processSubscriptionRequest(inputFile, outputFile, taskId)