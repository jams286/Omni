import requests
import json
import pandas as pd

URL = "localhost/LSCommerceService/UCJson.svc"
prices = '/ReplEcommBasePrices'

maxKey = "0"
lastKey = "0"
fullReplication = 1
recordsRemaining = 0
batchSize = 20000
fileName = "ReplEcommBasePrices.csv"
json_data = {}
df = pd.DataFrame()

payload = json.dumps({
  "replRequest": {
    "BatchSize": batchSize,
    "FullReplication": fullReplication,
    "LastKey": lastKey,
    "MaxKey": maxKey,
    "StoreId": "E002",
    "TerminalId": ""
  }
})
headers = {
  'Content-Type': 'application/json'
}

def firstRun():
    global recordsRemaining, lastKey, maxKey, df
    response = requests.request("POST", URL+prices, headers=headers, data=payload)  
    if response.status_code == 200:
        json_data = response.json()
        recordsRemaining = int(json_data["ReplEcommBasePricesResult"]["RecordsRemaining"])
        maxKey = json_data["ReplEcommBasePricesResult"]["MaxKey"]
        lastKey = json_data["ReplEcommBasePricesResult"]["LastKey"]
        df = pd.DataFrame(json_data["ReplEcommBasePricesResult"]["Prices"])
        print(f'LastKey {lastKey}, MaxKey {maxKey}, recordsRemaining {recordsRemaining}')        

def main():
    
    global maxKey, lastKey, recordsRemaining, df
    while recordsRemaining != 0:
        payload = json.dumps({
        "replRequest": {
        "BatchSize": batchSize,
        "FullReplication": fullReplication,
        "LastKey": lastKey,
        "MaxKey": maxKey,
        "StoreId": "E002",
        "TerminalId": ""
        }
        })
        response = requests.request("POST", URL+prices, headers=headers, data=payload)  
        if response.status_code == 200:
            json_data = response.json()
            recordsRemaining = int(json_data["ReplEcommBasePricesResult"]["RecordsRemaining"])
            maxKey = json_data["ReplEcommBasePricesResult"]["MaxKey"]
            lastKey = json_data["ReplEcommBasePricesResult"]["LastKey"]
            print(f'LastKey {lastKey}, MaxKey {maxKey}, recordsRemaining {recordsRemaining}')        
            df2 = pd.DataFrame(json_data["ReplEcommBasePricesResult"]["Prices"])
            df = pd.concat([df, df2])

if __name__ == '__main__':
    # main()
    firstRun()
    main()
    df.to_csv(fileName, index=False)


