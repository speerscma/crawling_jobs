import requests
import json
from fake_useragent import UserAgent
from random import randrange
import time
from datetime import datetime as dt


# define headers
def gen_headers():
    headers = {
        "authority":        "www.jobs.ch",
        "method":           "GET",
        "scheme":           "https",
        "accept":           "application/json",
        "accept-encoding":  "gzip, deflate, br",
        "accept-language":  "en",
        "user-agent":       UserAgent().chrome,
    }
    return headers


# regions:
region_dict = {
    19: "City of Zurich / Lake Zurich",
    20: "Zurich Oberland",
    22: "Zurich Unterland / Limmattal",
    25: "Region of Winterthur / Schaffhausen",
    17: "Wil / Toggenburg",
    18: "Thurgau / Lake Constance",
    21: "St Gall / Appenzell",
    23: "Rheintal / FL / Sargans / Linth",
    24: "Graubünden",
    12: "Mittelland (AG / SO)",
    13: "Region of Bern",
    5:  "Abroad",
    4:  "Ticino",
    10: "Region of Vaud / Valais",
    9:  "Region of Fribourg",
    8:  "Region of Neuchâtel / Jura",
    6:  "Region of Geneva",
    16: "Region of Oberwallis",
    15: "Central Switzerland",
    14: "Region of Basel"
}

segment_dict = {
    "kmu":  "Small & medium",
    "gu":   "Large"
}

# TODO: -> bei mehr als 2k Einträgen aufsplitten



def execute_request(url, page, num_pages, job_id_set):
    response = requests.get(url=url, headers=gen_headers())
    print(f"[{dt.now().strftime('%H:%M:%S')}] {page}/{num_pages}: {url}")
    print(response.status_code)
    documents = json.loads(response.content)["documents"]
    for entry in documents:
        job_id_set.add(entry["job_id"])
    sleep_time = randrange(2, 7)
    print(f"Sleeping: {sleep_time}")
    time.sleep(sleep_time)




# get total page count
job_id_set = set()

for region_id in region_dict.keys():
    print(f"Starting crawling region_id: {region_id}: {region_dict[region_id]}")

    url = f"https://www.jobs.ch/api/v1/public/search?category-ids%5B%5D=106&region-ids%5B%5D={region_id}&page=1&rows=20"
    response = requests.get(url=url, headers=gen_headers())

    print(response.status_code)
    content = json.loads(response.content)
    total_hits = content["total_hits"]


    if total_hits > 2000:
        print("More than 2k hits -> use segmentation based on company size")
        for segment in segment_dict.keys():
            print(segment)
            url = f"https://www.jobs.ch/api/v1/public/search?category-ids%5B%5D=106&region-ids%5B%5D={region_id}&company-segments%5B%5D={segment}&page=1&rows=20"
            response = requests.get(url=url, headers=gen_headers())
            print(response.status_code)
            total_hits = json.loads(response.content)["total_hits"]
            print(f'{region_id} & {segment} total_hits: {total_hits}')
            if total_hits > 2000:
                print("ERROR")
                break
            else:
                num_pages = json.loads(response.content)["num_pages"]

                for page in range(1, num_pages + 1):
                    url = f"https://www.jobs.ch/api/v1/public/search?category-ids%5B%5D=106&region-ids%5B%5D={region_id}&company-segments%5B%5D={segment}&page={page}&rows=20"
                    execute_request(url, page, num_pages, job_id_set)

    else:
        num_pages = json.loads(response.content)["num_pages"]
        for page in range(1, num_pages + 1):
            url = f"https://www.jobs.ch/api/v1/public/search?category-ids%5B%5D=106&region-ids%5B%5D={region_id}&page={page}&rows=20"
            execute_request(url, page, num_pages, job_id_set)





with open("job_id_list.json", "w") as f:

    json.dump(list(job_id_set), f)




