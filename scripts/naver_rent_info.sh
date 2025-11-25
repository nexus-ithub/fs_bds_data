

source /data/fs_bds_ai/fs_bds_data/venv/bin/activate
echo "naver rent info script started at $(date)"
setsid nohup python /data/fs_bds_ai/fs_bds_data/publicdata/collect_rent_info.py --db /data/fs_bds_ai/fs_bds_data/config/config.json 1> /dev/null 2>&1 &
echo "naver rent info script finished at $(date)"
deactivate


