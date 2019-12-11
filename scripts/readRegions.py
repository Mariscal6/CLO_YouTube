import json

def region_dict():
    dict_ = dict()
    with open("youtubeRegions.json") as file:
        data = json.load(file)
        for item in data["items"]:
            code = item["snippet"]["gl"]
            country = item["snippet"]["name"]
            dict_[code] = country
    return dict_