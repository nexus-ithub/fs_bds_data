

source /data/fs_bds_ai/fs_bds_data/venv/bin/activate
echo "land deal script started at $(date)"
nohup python /data/fs_bds_ai/fs_bds_data/write_land_deal.py --db /data/fs_bds_ai/fs_bds_data/config/config.json --range update --logdir /data/fs_bds_ai/fs_bds_data/logs/ 1> /dev/null 2>&1
echo "land deal script finished at $(date)"
deactivate

