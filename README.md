# MRI DICOM Anonymization & QC

This repository provides a small pipeline to:

1. **Anonymize DICOM MRI data**
2. **Run quality checks (QC)** on anonymized scans
3. **Do both in a single run**

It is designed for quick checks on folders containing **any number of MRI scans** (multiple series/studies), and for fast anonymization of whole MRI study sessions.

> **Runtime:** For one MRI **study session date** (all series from a single session), anonymization **+** QC typically takes **< 30 seconds** on a standard workstation.

---

## Features

- **Two modules, three ways to run**
  - **Anonymization only**
  - **QC only** (check if an existing folder is anonymized correctly)
  - **Anonymization + QC** (anonymize, then immediately check)
- **Configurable via `config.yaml`**
  - Turn anonymization and QC on/off
  - Specify input and output folders
  - Select which identifiers to keep/drop
- **QC of anonymization**
  - Checks whether the expected **non-identifying technical parameters** are **retained**
  - Uses `keep_keywords.json` to define which DICOM tags must be kept
  - Outputs JSON reports that you can quickly skim
- **Direct anonymization**
  - Uses `dicom-anonymizer` and `pydicom`
  - Reads all configuration from `config.yaml`
- **Utility for extensions**
  - Ensures files have `.dcm` extensions if needed

---

## Repository structure

- `anonymization_qc.py`  
  Main script. Uses the configuration in `config.yaml` to:
  - (Optionally) anonymize DICOM files
  - (Optionally) run QC on (already) anonymized files  

- `config.yaml`  
  Central configuration file for **anonymization** and **QC**.  
  👉 **Only change the lines that are explicitly marked with `# To Change`.**

- `keep_keywords.json`  
  List of **non-identifying** DICOM tags that should be **retained** in the anonymized dataset. QC uses this to verify that important technical metadata is not removed.

- `requirements.txt`  
  Python dependencies:
  - `pydicom`
  - `dicom-anonymizer`
  - `PyYAML`

---

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/ZohaibS1995/mri_qc_anonymize.git
   cd mri_qc_anonymize

2. (Recommended) Create a virtual environment and activate it.
3. Install dependencies:

    ```bash
   pip install -r requirements.txt
