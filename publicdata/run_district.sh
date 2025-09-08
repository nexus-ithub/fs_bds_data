
echo $1

nohup python3 $1 --type district_polygon
nohup python3 $1 --type district_foot_traffic
nohup python3 $1 --type district_office_workers
nohup python3 $1 --type district_resident
nohup python3 $1 --type district_resident_alltime
nohup python3 $1 --type district_foot_traffic_seoul
nohup python3 $1 --type leg_dong_codes