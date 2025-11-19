import os
import csv
import json
import traceback
from datetime import datetime
from typing import List, Dict, Callable

import pydicom
from pydicom.datadict import tag_for_keyword
from dicomanonymizer import anonymize, keep
import yaml


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_keep_keywords(json_path: str) -> List[str]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    keywords = data.get("keywords", [])
    if not isinstance(keywords, list):
        raise ValueError(f"'keywords' in {json_path} must be a list")
    return keywords


def build_keep_tags_from_keywords(keywords: List[str]) -> List[tuple]:
    tags: List[tuple] = []
    for kw in keywords:
        raw_tag = tag_for_keyword(kw)
        if raw_tag is None:
            print(f"[WARN] Unknown DICOM keyword in keep list: {kw}")
            continue
        try:
            group = raw_tag.group
            element = raw_tag.element
        except AttributeError:
            group = raw_tag >> 16
            element = raw_tag & 0xFFFF
        tags.append((group, element))
    return tags


def anonymize_file(
    in_path: str,
    out_path: str,
    anon_rules: Dict[tuple, Callable],
    delete_private_tags: bool,
) -> None:
    anonymize(
        in_path,
        out_path,
        anon_rules,
        delete_private_tags=delete_private_tags,
    )


def anonymize_tree(
    input_root: str,
    output_root: str,
    error_csv: str,
    anon_rules: Dict[tuple, Callable],
    delete_private_tags: bool,
    valid_exts: set,
) -> None:
    errors: List[dict] = []
    num_processed = 0
    num_failed = 0

    for root, _, files in os.walk(input_root):
        rel_path = os.path.relpath(root, input_root)
        out_dir = os.path.join(output_root, rel_path)
        os.makedirs(out_dir, exist_ok=True)

        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if valid_exts and ext not in valid_exts:
                continue

            in_path = os.path.join(root, fname)
            out_path = os.path.join(out_dir, fname)

            try:
                anonymize_file(in_path, out_path, anon_rules, delete_private_tags)
                num_processed += 1
            except Exception as e:
                num_failed += 1
                errors.append({
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "input_path": in_path,
                    "output_path": out_path,
                    "error_type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc().strip().replace("\n", " | "),
                })
                print(f"[ERROR] {in_path} -> {e}")

    if errors:
        write_errors_csv(error_csv, errors)
        print(f"Wrote {len(errors)} errors to {error_csv}")
    else:
        print("No errors encountered during anonymization.")

    print(f"Anonymization summary: processed={num_processed}, failed={num_failed}")


def write_errors_csv(path: str, rows: List[dict]) -> None:
    fieldnames = ["timestamp", "input_path", "output_path", "error_type", "message", "traceback"]
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# PatientID deliberately not included here so it is not treated as PII
DEFAULT_PII_KEYWORDS = [
    "PatientName",
    "OtherPatientIDs",
    "PatientBirthDate",
    "PatientSex",
    "PatientAddress",
    "PatientTelephoneNumbers",
    "PatientBirthName",
    "PatientMotherBirthName",
    "AccessionNumber",
    "ReferringPhysicianName",
    "PerformingPhysicianName",
    "OperatorsName",
    "InstitutionName",
    "InstitutionAddress",
]


def run_qc_check(
    qc_root: str,
    valid_exts: set,
    qc_json_path: str,
    qc_json_detailed_path: str,
    keep_patient_id: bool,
    keep_series_instance_uid: bool,
    pii_keywords: List[str],
) -> None:
    summary_counts = {kw: 0 for kw in pii_keywords}
    num_files_checked = 0
    num_files_with_unexpected_pii = 0
    violations = []

    series_data: Dict[str, dict] = {}
    file_entries: List[dict] = []

    for root, _, files in os.walk(qc_root):
        dicom_files = []
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if valid_exts and ext not in valid_exts:
                continue
            dicom_files.append(fname)

        if not dicom_files:
            continue

        rel_path = os.path.relpath(root, qc_root)
        series_key = rel_path
        folder_name = os.path.basename(root)
        if rel_path == ".":
            folder_name = os.path.basename(qc_root.rstrip(os.sep))

        if series_key not in series_data:
            series_data[series_key] = {
                "series_name": folder_name,
                "relative_path": rel_path,
                "series_path": root,
                "num_files": 0,
                "pii_values": {kw: set() for kw in pii_keywords},
            }

        series_info = series_data[series_key]

        for fname in dicom_files:
            path = os.path.join(root, fname)
            try:
                ds = pydicom.dcmread(path, stop_before_pixels=True)
            except Exception as e:
                print(f"[QC WARN] Could not read {path}: {e}")
                continue

            num_files_checked += 1
            series_info["num_files"] += 1

            file_pii = {}

            for kw in pii_keywords:
                val = getattr(ds, kw, None)
                if val is None:
                    continue
                val_str = str(val).strip()
                if not val_str:
                    continue

                summary_counts[kw] += 1
                series_info["pii_values"][kw].add(val_str)
                file_pii[kw] = val_str

            if file_pii:
                file_entries.append({
                    "file": path,
                    "series_relative_path": rel_path,
                    "series_name": folder_name,
                    "pii_values": file_pii,
                })

    series_list = []
    for series_key in sorted(series_data.keys()):
        info = series_data[series_key]
        pii_out = {}
        for kw, values in info["pii_values"].items():
            if values:
                pii_out[kw] = sorted(values)
        series_list.append({
            "series_name": info["series_name"],
            "relative_path": info["relative_path"],
            "series_path": info["series_path"],
            "num_files": info["num_files"],
            "pii_values": pii_out,
        })

    rules_block = {
        "keep_patient_id": keep_patient_id,
        "keep_series_instance_uid": keep_series_instance_uid,
        "pii_keywords": pii_keywords,
    }
    summary_block = {
        "num_files_checked": num_files_checked,
        "num_series_with_dicoms": len(series_list),
        "num_files_with_unexpected_pii": num_files_with_unexpected_pii,
        "pii_counts_by_keyword": summary_counts,
    }

    qc_data = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "qc_root": qc_root,
        "rules": rules_block,
        "summary": summary_block,
        "series": series_list,
        "violations": violations,
    }

    dirpath = os.path.dirname(qc_json_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(qc_json_path, "w", encoding="utf-8") as f:
        json.dump(qc_data, f, indent=2)

    qc_data_detailed = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "qc_root": qc_root,
        "rules": rules_block,
        "summary": summary_block,
        "files": file_entries,
    }

    dirpath2 = os.path.dirname(qc_json_detailed_path)
    if dirpath2:
        os.makedirs(dirpath2, exist_ok=True)
    with open(qc_json_detailed_path, "w", encoding="utf-8") as f:
        json.dump(qc_data_detailed, f, indent=2)

    print(f"[QC] Wrote PII series JSON: {qc_json_path}")
    print(f"[QC] Wrote PII detailed JSON: {qc_json_detailed_path}")
    print(
        f"PII QC summary: files_checked={num_files_checked}, "
        f"series_with_dicoms={len(series_list)}"
    )


def run_keep_keywords_check(
    qc_root: str,
    valid_exts: set,
    keep_json_path: str,
    keep_keywords: List[str],
    keep_patient_id: bool,
    keep_series_instance_uid: bool,
) -> None:
    summary_counts = {kw: 0 for kw in keep_keywords}
    num_files_checked = 0

    series_data: Dict[str, dict] = {}
    file_entries: List[dict] = []

    for root, _, files in os.walk(qc_root):
        dicom_files = []
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if valid_exts and ext not in valid_exts:
                continue
            dicom_files.append(fname)

        if not dicom_files:
            continue

        rel_path = os.path.relpath(root, qc_root)
        series_key = rel_path
        folder_name = os.path.basename(root)
        if rel_path == ".":
            folder_name = os.path.basename(qc_root.rstrip(os.sep))

        if series_key not in series_data:
            series_data[series_key] = {
                "series_name": folder_name,
                "relative_path": rel_path,
                "series_path": root,
                "num_files": 0,
                "keep_values": {kw: set() for kw in keep_keywords},
            }

        series_info = series_data[series_key]

        for fname in dicom_files:
            path = os.path.join(root, fname)
            try:
                ds = pydicom.dcmread(path, stop_before_pixels=True)
            except Exception as e:
                print(f"[KEEP-QC WARN] Could not read {path}: {e}")
                continue

            num_files_checked += 1
            series_info["num_files"] += 1

            file_vals = {}

            for kw in keep_keywords:
                val = getattr(ds, kw, None)
                if val is None:
                    continue
                val_str = str(val).strip()
                if not val_str:
                    continue

                summary_counts[kw] += 1
                series_info["keep_values"][kw].add(val_str)
                file_vals[kw] = val_str

            if file_vals:
                file_entries.append({
                    "file": path,
                    "series_relative_path": rel_path,
                    "series_name": folder_name,
                    "keep_values": file_vals,
                })

    series_list = []
    for series_key in sorted(series_data.keys()):
        info = series_data[series_key]
        keep_out = {}
        for kw, values in info["keep_values"].items():
            if values:
                keep_out[kw] = sorted(values)
        series_list.append({
            "series_name": info["series_name"],
            "relative_path": info["relative_path"],
            "series_path": info["series_path"],
            "num_files": info["num_files"],
            "keep_values": keep_out,
        })

    rules_block = {
        "keep_patient_id": keep_patient_id,
        "keep_series_instance_uid": keep_series_instance_uid,
        "keep_keywords": keep_keywords,
    }
    summary_block = {
        "num_files_checked": num_files_checked,
        "num_series_with_dicoms": len(series_list),
        "keep_counts_by_keyword": summary_counts,
    }

    keep_qc_data = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "qc_root": qc_root,
        "rules": rules_block,
        "summary": summary_block,
        "series": series_list,
        "files": file_entries,
    }

    dirpath = os.path.dirname(keep_json_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(keep_json_path, "w", encoding="utf-8") as f:
        json.dump(keep_qc_data, f, indent=2)

    print(f"[KEEP-QC] Wrote keep-keywords JSON: {keep_json_path}")
    print(
        f"Keep-keywords QC summary: files_checked={num_files_checked}, "
        f"series_with_dicoms={len(series_list)}"
    )


def main(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)
    config_dir = os.path.dirname(os.path.abspath(config_path))

    identifiers_cfg = config.get("identifiers", {}) or {}
    keep_patient_id = bool(identifiers_cfg.get("keep_patient_id", True))
    keep_series_instance_uid = bool(identifiers_cfg.get("keep_series_instance_uid", True))

    keep_keywords_file = config.get("keep_keywords_file", "keep_keywords.json")
    if not os.path.isabs(keep_keywords_file):
        keep_keywords_file = os.path.join(config_dir, keep_keywords_file)
    base_keep_keywords = load_keep_keywords(keep_keywords_file)

    anonymization_cfg = config.get("anonymization", {}) or {}
    run_anonymization = bool(anonymization_cfg.get("enabled", True))

    an_input_root = anonymization_cfg.get("input_root")
    an_output_root = anonymization_cfg.get("output_root")

    if run_anonymization:
        if not an_input_root or not an_output_root:
            raise ValueError(
                "Anonymization is enabled, but 'anonymization.input_root' or "
                "'anonymization.output_root' is missing in config.yaml"
            )
        an_input_root = os.path.abspath(an_input_root)
        an_output_root = os.path.abspath(an_output_root)

    an_valid_exts = set(anonymization_cfg.get("valid_extensions", [".dcm", ".dicom"]))
    delete_private_tags = bool(anonymization_cfg.get("delete_private_tags", True))
    extra_keep_keywords = anonymization_cfg.get("extra_keep_keywords", []) or []

    keep_keywords_all = list(base_keep_keywords) + list(extra_keep_keywords)
    if keep_patient_id and "PatientID" not in keep_keywords_all:
        keep_keywords_all.append("PatientID")
    if keep_series_instance_uid and "SeriesInstanceUID" not in keep_keywords_all:
        keep_keywords_all.append("SeriesInstanceUID")

    keep_tags = build_keep_tags_from_keywords(keep_keywords_all)
    anon_rules = {tag: keep for tag in keep_tags}

    an_log_dir = None
    error_csv = None
    if run_anonymization:
        an_log_dir = anonymization_cfg.get("log_dir") or "logs_anonymization"
        if not os.path.isabs(an_log_dir):
            an_log_dir = os.path.join(an_output_root, an_log_dir)
        error_csv_filename = anonymization_cfg.get("error_csv_filename", "errors.csv")
        error_csv = os.path.join(an_log_dir, error_csv_filename)
        os.makedirs(an_log_dir, exist_ok=True)

    qc_cfg = config.get("qc", {}) or {}
    run_qc = bool(qc_cfg.get("enabled", True))
    inspect_keep_keywords = bool(qc_cfg.get("inspect_keep_keywords", False))

    qc_root = qc_cfg.get("root")
    if run_qc:
        if not qc_root:
            if run_anonymization:
                qc_root = an_output_root
            else:
                raise ValueError(
                    "QC is enabled, but 'qc.root' is missing in config.yaml and "
                    "anonymization is disabled (no default QC root)."
                )
        qc_root = os.path.abspath(qc_root)

    qc_valid_exts = set(
        qc_cfg.get("valid_extensions", list(an_valid_exts) if run_anonymization else [".dcm", ".dicom"])
    )

    qc_log_dir = None
    qc_json_path = None
    qc_json_detailed_path = None
    keep_keywords_json_path = None
    if run_qc:
        qc_log_dir = qc_cfg.get("log_dir") or "logs_qc"
        if not os.path.isabs(qc_log_dir):
            qc_log_dir = os.path.join(qc_root, qc_log_dir)

        qc_json_filename = qc_cfg.get("qc_json_filename", "anonymization_qc.json")
        qc_json_path = os.path.join(qc_log_dir, qc_json_filename)

        qc_json_detailed_filename = qc_cfg.get(
            "qc_json_detailed_filename", "anonymization_qc_detailed.json"
        )
        qc_json_detailed_path = os.path.join(qc_log_dir, qc_json_detailed_filename)

        keep_keywords_json_filename = qc_cfg.get(
            "keep_keywords_json_filename", "anonymization_keep_keywords_qc.json"
        )
        keep_keywords_json_path = os.path.join(qc_log_dir, keep_keywords_json_filename)

        os.makedirs(qc_log_dir, exist_ok=True)

    pii_keywords = qc_cfg.get("pii_keywords") or DEFAULT_PII_KEYWORDS
    pii_keywords = [kw for kw in pii_keywords if kw != "PatientID"]

    print("============================================================")
    print("CONFIG SUMMARY")
    print("============================================================")
    print(f"Keep keywords file : {keep_keywords_file}")
    print(f"Keep keywords      : {keep_keywords_all}")
    print(f"keep_patient_id    : {keep_patient_id}")
    print(f"keep_series_uid    : {keep_series_instance_uid}")
    print("============================================================\n")

    print("ANONYMIZATION")
    print(f"enabled            : {run_anonymization}")
    if run_anonymization:
        print(f"input_root         : {an_input_root}")
        print(f"output_root        : {an_output_root}")
        print(f"log_dir            : {an_log_dir}")
        print(f"valid_extensions   : {sorted(an_valid_exts)}")
        print(f"delete_private     : {delete_private_tags}")
    print("============================================================\n")

    if run_anonymization:
        anonymize_tree(
            input_root=an_input_root,
            output_root=an_output_root,
            error_csv=error_csv,
            anon_rules=anon_rules,
            delete_private_tags=delete_private_tags,
            valid_exts=an_valid_exts,
        )
    else:
        print("Anonymization disabled.\n")

    print("QC")
    print(f"PII QC enabled     : {run_qc}")
    if run_qc:
        print(f"qc_root            : {qc_root}")
        print(f"qc_log_dir         : {qc_log_dir}")
        print(f"qc_json            : {qc_json_path}")
        print(f"qc_json_detailed   : {qc_json_detailed_path}")
        print(f"pii_keywords       : {pii_keywords}")
        print(f"inspect_keep_keys  : {inspect_keep_keywords}")
        if inspect_keep_keywords:
            print(f"keep_json          : {keep_keywords_json_path}")
    print("============================================================\n")

    if run_qc:
        if not os.path.isdir(qc_root):
            print(f"QC root '{qc_root}' does not exist. Skipping PII QC.")
        else:
            run_qc_check(
                qc_root=qc_root,
                valid_exts=qc_valid_exts,
                qc_json_path=qc_json_path,
                qc_json_detailed_path=qc_json_detailed_path,
                keep_patient_id=keep_patient_id,
                keep_series_instance_uid=keep_series_instance_uid,
                pii_keywords=pii_keywords,
            )

        if inspect_keep_keywords:
            keep_qc_root = qc_root  # works whether data is anonymised or not
            if not os.path.isdir(keep_qc_root):
                print(f"Keep-keywords QC root '{keep_qc_root}' does not exist. Skipping.")
            else:
                run_keep_keywords_check(
                    qc_root=keep_qc_root,
                    valid_exts=qc_valid_exts,
                    keep_json_path=keep_keywords_json_path,
                    keep_keywords=sorted(set(keep_keywords_all)),
                    keep_patient_id=keep_patient_id,
                    keep_series_instance_uid=keep_series_instance_uid,
                )
    else:
        print("QC disabled.\n")


if __name__ == "__main__":
    main()
