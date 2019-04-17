# mysql-quota
Simple Python script to enforce storage quotas on MySQL databases

## Important Note
Currently, we are not storing permissions prior to limits being placed. This means if you have any users without insert, create, or update privileges, they will gain those permissions upon the database limits being lifted.

## Cron Job Example
Will run script every minute: 
``* * * * * /usr/bin/python3 /home/quota/mysql-quota/src/main.py > /home/quota/mysql-quota/src/quota.log``
