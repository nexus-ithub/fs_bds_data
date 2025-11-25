
echo $1

nohup python3 $1 --type building_leg_headline
nohup python3 $1 --type building_floor_info
nohup python3 $1 --type building_sub_addr
