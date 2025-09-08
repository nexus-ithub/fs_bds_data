
echo $1

nohup python3 $1 --type address
nohup python3 $1 --type building_addr
nohup python3 $1 --type land_info
nohup python3 $1 --type land_char_info
nohup python3 $1 --type land_usage_info
nohup python3 $1 --type address_polygon
nohup python3 $1 --type individual_announced_price