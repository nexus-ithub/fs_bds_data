import csv
import os
import shutil
import zipfile


def read_csv(file_name, delimiter='|', encoding='cp949', quoting=0):
    with open(file_name, "r", encoding=encoding) as f:
        reader = csv.reader(f, delimiter=delimiter, quoting=quoting)
        for row in reader:
            yield row


def merge_split_zip_files(input_dir, output_path):
    # print(f"Merging zip files...{input_dir} {output_path}")
    output_zip_path = os.path.join(input_dir, "output.zip")

    # print(f"remove output_zip_path {output_zip_path}")
    if os.path.exists(output_zip_path):
        os.remove(output_zip_path)
    # print(f"remove output_path directory {output_path}")
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    split_files = sorted([f for f in os.listdir(input_dir) if f.endswith(('.zip', '.z01', '.z02', '.z03', '.z04'))])

    with open(output_zip_path, 'wb') as outfile:
        # print("Writing from {}".format(split_files))
        for split_file in split_files:
            split_file_path = os.path.join(input_dir, split_file)
            with open(split_file_path, 'rb') as infile:
                outfile.write(infile.read())
    # print("output_zip_path {}".format(output_zip_path))

    os.makedirs(output_path, exist_ok=True)

    os.system(f"unzip {output_zip_path} -d {output_path}")

    os.remove(output_zip_path)

