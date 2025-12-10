# *TDATA/CKAN-INIT-SCRIPTS*

> Collection of utility scripts to perform batch actions through the CKAN API

---

## Objectives

- Have an easy way to set up and fill Tdata

---

## Funding Information

This research project is supported by:

- **Funding organization/institution:**  Conselleria de Educación, Universidades y Empleo de la Generalitat Valenciana
- **Program or grant:** Subvenciones a grupos de investigación consolidados
- **Project code/reference:** CIAICO/2022/019
- **Duration:** [01/01/2023 – 31/12/2025]  

---

## Technology

- Python

---

## Installation and Usage

To use the scripts copy the dotenv file into an ".env" file and fill up the variables.
The sripts requirements are listed in the requirements file.

### init_orgs.py
Creates organizations from a data file (csv file at "./data/organizations_list.csv").
The organization images should be stored at "./data/image".

### init_groups.py
Creates groups from data file (csv file at "./data/groups_list.csv").
The group images should be stored at "./data/image".

### init_vocabularies.py
Created a vocabulary

### Dataset_importer.py
Isert all the datasets on CKAN

### set_column_labels.py
Add labels to columns

---

## Authors / Contributors
- Lucía de Espona Pernas – [@espona](https://github.com/espona)  

---

## License

This project is distributed under the [MIT License](LICENSE).

---

## 💬 Contact

For questions, collaborations, or further information:

📧 [wake@dlsi.ua.es](mailto:wake@dlsi.ua.es)  
🌐 [Wake Research group](https://wake.dlsi.ua.es/)
