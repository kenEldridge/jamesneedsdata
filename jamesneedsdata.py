import click
import pdb
import pandas as pd
import urllib3
from dotenv import load_dotenv
import os
import time
import json

def text_query(search_text):
    # time.sleep(.4)
    http = urllib3.PoolManager()
    resp = None
    try:
        resp = http.request(
            "POST",
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "Content-Type": "application/json"
                , "X-Goog-Api-Key": os.getenv("PLACES_API_KEY")
                , "X-Goog-FieldMask": "places.nationalPhoneNumber,places.id,places.displayName,places.formattedAddress,places.types,places.websiteUri"
            },
            json={
                "textQuery": search_text
            }
        )
    except Exception as e:
        print(e)

    try:
        print(f"{resp.status} - {search_text} {resp.json()['places'][0]['id']}")
    except Exception as e:
        print(f"{search_text}, {e}")

    if resp:
        return (resp.json())
    else:
        print("return nothing")
        return None

@click.command()
@click.option('--file', help='path to file to process')
def main(file):
    df = pd.read_csv(file)
    print(f"{file} shape {df.shape}")
    df["city-state"] = df['Name'] + " " + df['State']
    df["text-query"] = df.apply(lambda x: f"{x['city-state']} public works", axis=1)
    df["places-resp"] = df["text-query"].apply(lambda x: text_query(x))
    df.to_csv(f"{file[:-4]}_save-point.csv")
    # pdb.set_trace()
    # df = pd.read_csv('western_states_save-point-sample.csv', converters={'places-resp': json.loads})
    df["phone"] = df["places-resp"].apply(lambda x: list(x["places"][0].values())[2] if "places" in x.keys() and len(x["places"][0]) >= 3 else "")
    df["address"] = df["places-resp"].apply(lambda x: list(x["places"][0].values())[3] if "places" in x.keys() and len(x["places"][0]) >= 4  else "")
    df["displayName"] = df["places-resp"].apply(lambda x: list(x["places"][0].values())[4] if "places" in x.keys() and len(x["places"][0]) >= 5 else "")
    
    del df["places-resp"]
    del df["text-query"]
    df.to_csv(f"{file[:-4]}_flush.csv", index=False)

if __name__ == '__main__':
    load_dotenv()
    main()
    print("done.")