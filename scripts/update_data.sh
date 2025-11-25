

source /data/fs_bds_ai/fs_bds_data/venv/bin/activate
echo "update data script started at $(date)"
setsid nohup python /data/fs_bds_ai/fs_bds_data/publicdata/update_data.py 1> /dev/null 2>&1 &
echo "update data script finished at $(date)"
deactivate

