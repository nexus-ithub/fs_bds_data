

source /data/fs_bds_ai/fs_bds_data/venv/bin/activate
echo "download land & address files script started at $(date)"
setsid nohup bash /data/fs_bds_ai/fs_bds_data/publicdata/run_district.sh /data/fs_bds_ai/fs_bds_data/publicdata/data_download.py &
echo "download land & address files script finished at $(date)"
deactivate

