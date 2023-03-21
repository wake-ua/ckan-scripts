
import os, sys
from common_importer import import_datasets

# global variables and default values
ORG_DIR = {

    # CKAN
    "opendata.alcoi.org": "alcoi",
    "datosabiertos.torrent.es": "torrent",
    "datosabiertos.sagunto.es": "sagunto",
    "dadesobertes.seu-e.cat": "aoc",
    "dadesobertes.gva.es": "gva",

    # OpenDataSoft
    "valencia.opendatasoft.com": "valencia",
    "datosabiertos.dipcas.es": "dipcas",

    # INE
    "servicios.ine.es": "ine"
}

BASE_DIR = "./data"


def main() -> int:

    # input parameters
    selected_package = None

    if len(sys.argv) > 1:
        input_dirs = [sys.argv[1]]
        if len(sys.argv) > 2:
            selected_package = sys.argv[2]
    else:
        input_dirs = [os.path.join(BASE_DIR, subdir) for subdir in ORG_DIR.keys()]

    for input_dir in input_dirs:
        org = ORG_DIR[input_dir.rsplit('/', 1)[-1]]
        print(org)
        import_datasets(input_dir, org, selected_package)
    return 0


if __name__ == '__main__':
    sys.exit(main())
