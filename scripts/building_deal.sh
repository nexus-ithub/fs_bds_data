

source /data/fs_bds_ai/fs_bds_data/venv/bin/activate
echo "building deal script started at $(date)"
nohup python /data/fs_bds_ai/fs_bds_data/write_building_deal.py --db /data/fs_bds_ai/fs_bds_data/config/config.json --range update --logdir /data/fs_bds_ai/fs_bds_data/logs/ 1> /dev/null 2>&1
echo "building deal script finished at $(date)"
deactivate


