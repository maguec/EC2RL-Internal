# input params
# Input File path to be processed

# Import pandas
import pandas as pd
import sys, math
from pathlib import Path

def getColumns():
    """Return the sheet columns
    Args:
    Returns:
        An array with the sheet columns
    """
    cols = ['Region', 'DB Name', 'Memory Limit (GB)', 'Ops/Sec', 'HA?', 'Min Network (Mbps)', 
            'Packets', 'Cluster API?', 'Limited HA?']
    return cols

def processFile(inputFile, outputFile):
    """Process the entire input file
    Args:
        inputFile: the file path to be processed
        outputFile: the file path for the processed file
    Returns:
        None
    """
    outIndex = 0
    i = 0
    while i < len(inputDF.index):
        # check if end of data
        if not isinstance(inputDF['NodeId'][i], str):
            break

        print(inputDF['NodeId'][i])
        i = processDB(i, outIndex)
        outIndex = outIndex + 1
    
    writer = pd.ExcelWriter(outputFile, engine='xlsxwriter')
    outputDF.to_excel(writer, 'Sheet1')
    writer.save()

def processDB(index, outIndex):
    """Calculate arguments for planner for a specific DBß
    Args:
        index:  the index of the DB in the dataframe
    Returns:
        The number of days between the expiration date and now.
    """
    clusterId = inputDF['ClusterId'][index]
    i = index
    ops = 0
    dbSize = 0
    network = 0
    packets = 0
    numberOfReplicas = 0

    # if ClusterId is not empty this is clustered
    clustered = 0
    if isinstance(clusterId, str) and clusterId != '':
        clustered = 1

    # iterate the records with the same cluster id
    # check for num of replicas
    if clustered and "-0001" in inputDF['NodeId'][i]:
        nodeName = inputDF['NodeId'][index].partition("-001")[0]
        while isinstance(inputDF['NodeId'][i], str) and nodeName in inputDF['NodeId'][i]:
            numberOfReplicas = numberOfReplicas + 1
            i = i +1
    
    i = index
    # iterate the records with the same cluster id
    while clusterId == inputDF['ClusterId'][i]:
        # sum all ops from all cluster
        ops = ops + inputDF.iloc[i,17:26].sum()
        # check for max memory in cluster instances
        if inputDF['BytesUsedForCache (max over last week)'][i] > dbSize:
            dbSize = inputDF['BytesUsedForCache (max over last week)'][i]
        # sum all network from all cluster
        network = network + float(inputDF['NetworkBytesIn (max over last week)'][i]) + float(inputDF['NetworkBytesOut (max over last week)'][i])
        # sum all packets from all cluster
        packets = packets + float(inputDF['NetworkPacketsIn (max over last week)'][i]) + float(inputDF['NetworkPacketsOut (max over last week)'][i])    
        i = i + 1

    if numberOfReplicas:
        dbSize = dbSize * (i - index)
        dbSize = dbSize / numberOfReplicas

    # if i > index + 1:
    #     clustered = 1

    # if for each shard we have at least 2 replica
    # if clustered then loop first 3 NodeId check that have at least 3 nodes
    limitedHA = 1
    if clustered and numberOfReplicas > 1:
        limitedHA = 0
    # if clustered and "-0001" in inputDF['NodeId'][index]:
    #     nodeName = inputDF['NodeId'][index].partition("-001")[0]
    #     if isinstance(inputDF['NodeId'][index + 1], str) and isinstance(inputDF['NodeId'][index + 2], str):
    #         if nodeName in inputDF['NodeId'][index + 1] and nodeName in inputDF['NodeId'][index + 2]:
    #             limitedHA = 0

    clusterAPI = 0
    if '0001' in inputDF['NodeId'][index]:
        clusterAPI = 1

    dbSize = round(dbSize / 1024 /1024 / 1024, 2)
    dbSize = dbSize * (1+clustered)
    network = round(network * networkMultiplier, 2)
    packets = round(packets / 3600, 0)
    ops = ops / 3600 * opsMultiplier
    ops = round(ops, 0)
    dbname = inputDF['NodeId'][index]
    if isinstance(inputDF['ClusterId'][index], str):
        dbname = inputDF['ClusterId'][index]
        print(dbname)
    outputDF.loc[len(outputDF)] = [inputDF['Region'][index][:-1], dbname, 
                    dbSize, ops, clustered, network, packets, clusterAPI, limitedHA]
    if not clustered:
        i = i +1

    return (i)

# Load csv
inputFile=sys.argv[1]
opsMultiplier = 10
networkMultiplier = 0.000000002222  # byte/hour -> Mbps
outputFile = Path(inputFile).stem + "-out" + ".xlsx"

inputDF = pd.read_csv(inputFile)
inputDF.sort_values(by=['ClusterId'])
outputDF = pd.DataFrame(columns=getColumns())
processFile(inputFile, outputFile)


