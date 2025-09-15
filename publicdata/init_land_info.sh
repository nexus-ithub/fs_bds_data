
python3 update_data.py --mode manual --type individual_announced_price --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/AL_D151_11_20240802.zip
python3 update_data.py --mode manual --type individual_announced_price --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/AL_D151_11_20250120.zip
python3 update_data.py --mode manual --type individual_announced_price --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/AL_D151_11_20250623.zip

echo "Files in /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/ directory:"
ls -1 /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/ | sort | while read file; do
    echo "$file"
done

ls -1 /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/ | sort | while read file; do
    python3 update_data.py --mode manual --type land_info --file_path "/data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/$file"
done

# python3 update_data.py --mode manual --type land_info --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/AL_D003_11_20240104.zip
# python3 update_data.py --mode manual --type land_info --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/AL_D003_11_20240121.zip
# python3 update_data.py --mode manual --type land_info --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/AL_D003_11_20240209.zip
# python3 update_data.py --mode manual --type land_info --file_path /data/fs_bds_ai/fs_bds_data/publicdata/data/land_info/AL_D003_11_20250623.zip