# mysql-quota
This is a simple Python script to enforce storage quotas on MySQL databases. It should be noted though: **there are better ways to accomplish storage quotas**. For example: file system quotas or running each DB in a separate MySQL instance ran within their own VMs or containers with storage constraints. I created this script as a quick fix - a production system should implement a more proper solution.

## Important Note
Currently, we are not storing permissions prior to limits being placed. This means if you have any users without insert, create, or update privileges, they will gain those permissions upon the database limits being lifted.

## Cron Job Example
Will run script every minute: 
``* * * * * /usr/bin/python3 /home/quota/mysql-quota/src/main.py >> /home/quota/mysql-quota/src/quota.log``
