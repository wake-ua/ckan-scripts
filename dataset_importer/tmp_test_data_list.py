import sys, os, json


def read_dataset(file_path: str) -> dict:
    # read the dataset file and transform to json dict
    with open(file_path) as jsonfile:
        dataset = json.load(jsonfile)
        dataset = dataset.get("result", dataset)
    return dataset


def list_files(input_dir):
    for file in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file)
        # print(file_path)
        if file.startswith("meta_") and file.endswith(".json"):
            dataset = read_dataset(file_path)
            dataset["url"] = dataset["resources"][0]["downloadUrl"]
            print('{},"{}",{}'.format(dataset["id_portal"], dataset["title"].strip(), dataset["url"]))


def main() -> int:

    # input parameters
    base_dir = sys.argv[1]

    input_dirs = [f.path for f in os.scandir(base_dir) if f.is_dir()]

    for input_dir in input_dirs:
        print(input_dir)
        list_files(input_dir)
    return 0


if __name__ == '__main__':
    sys.exit(main())
