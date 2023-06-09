import json

if __name__ == "__main__":
    with open ("../download/IDELIST/CALIST.txt", "r") as f:
        json_string = f.read()
        json_object = json.loads(json_string)
    CA = [x for x in json_object]
    print(CA)