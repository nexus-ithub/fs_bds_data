

source /data/fs_bds_ai/fs_bds_data/venv/bin/activate
echo "download building files script started at $(date)"
setsid nohup bash /data/fs_bds_ai/fs_bds_data/publicdata/run_building.sh /data/fs_bds_ai/fs_bds_data/publicdata/data_download.py 
echo "download building files script finished at $(date)"
deactivate

