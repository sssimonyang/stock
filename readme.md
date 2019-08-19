安装 miniconda  
安装库  
conda install xlrd xlwt pandas aiohttp requests  
服务器需解封25端口  
服务器设置定时任务  
service crond start  
ctrantab -e  
0 18 * * * /root/main.sh >> /home/python/stock/log 2>&1 &  
