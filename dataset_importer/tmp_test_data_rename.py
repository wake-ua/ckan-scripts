import sys, os, json


def read_dataset(file_path: str) -> dict:
    # read the dataset file and transform to json dict
    with open(file_path) as jsonfile:
        dataset = json.load(jsonfile)
        dataset = dataset.get("result", dataset)
    return dataset


def rename_files(input_dir):

    for file in os.listdir(input_dir):
        new_name = None
        file_path = os.path.join(input_dir, file)
        if len(file)<23:
            print(file_path)
            if file.startswith("meta_") and file.endswith(".json"):
                dataset = read_dataset(file_path)
                new_name = os.path.join(input_dir,
                                        "meta_{}_{}.json".format(dataset["id_custom"], dataset["id_portal"][0:80]))
            elif file.startswith("all_") and file.endswith(".json"):
                dataset = read_dataset(file_path)
                dataset_name = dataset.get("name", "")
                if not dataset_name:
                    dataset_name = dataset["dataset"]["dataset_id"]
                new_name = os.path.join(input_dir, "{}_{}.json".format(file.split('.')[0],
                                        dataset_name[0:80]))

            if new_name:
                print(" - Rename {} => {}".format(file_path, new_name) )
                os.rename(file_path, new_name)


def main() -> int:

    # input parameters
    base_dir = sys.argv[1]

    input_dirs = [f.path for f in os.scandir(base_dir) if f.is_dir()]

    for input_dir in input_dirs:
        if not input_dir.endswith("servicios.ine.es"):
            print(input_dir)
            rename_files(input_dir)
    return 0


if __name__ == '__main__':
    sys.exit(main())
