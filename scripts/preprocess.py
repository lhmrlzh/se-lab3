import os
import re
import json
import pandas as pd

 
def process_data(data):
    result = data["result"]
    total = data["total"]
    records = data["records"]
    thead = data["thead"]
    tbody = data["tbody"]

    labels = []
    for label in thead:
        pat = re.search(r"<(.*)>", label)
        if pat:
            labels.append(label.replace(pat.group(0), ""))
        else:
            labels.append(label)

    datas = []
    for data in tbody:
        row = []
        for col in data:
            if col is None or len(col) == 0 or col == "*":
                row.append("--")
                continue
            pat = re.search(r"'原始值：.*'", col)
            if pat:
                row.append(pat.group(0).split("：")[1].strip("'"))
                continue

            pat = re.search(r">(.*?)<", col)
            if pat:
                col = pat.group(0).strip("><")

            row.append(col)
        datas.append(row)

    df = pd.DataFrame(datas, columns=labels)
    return df


def main(input_dir, output_dir):
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f)
                    df = process_data(data)
                    output_file = os.path.join(output_dir, os.path.relpath(file_path, input_dir)).replace(".json", ".csv")
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    df.to_csv(output_file, index=False, encoding="utf-8")


input_dir = "data/水质数据"
output_dir = "data/水质数据"
if __name__ == "__main__":
    main(input_dir, output_dir)
