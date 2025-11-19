import os

# CHANGE THIS to the top-level folder you want to process
root_dir = r"E:\Zohaib\anonymisation\05-02-2020_CT brain"

for dirpath, dirnames, filenames in os.walk(root_dir):
    for filename in filenames:
        # Skip files that already end with .dcm
        if filename.lower().endswith(".dcm"):
            continue

        old_path = os.path.join(dirpath, filename)
        new_name = filename + ".dcm"   # append .dcm to the existing name
        new_path = os.path.join(dirpath, new_name)

        # Avoid overwriting an existing file
        if os.path.exists(new_path):
            print(f"⚠️ Skipping (target exists): {old_path} -> {new_path}")
            continue

        print(f"Renaming: {old_path} -> {new_path}")
        os.rename(old_path, new_path)

print("Done.")
