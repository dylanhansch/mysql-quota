import mysql.connector
from mysql.connector import errorcode
import json
import re


def get_config():
    return json.load(open('config.json'))


def check_quota_format(value):
    if len(value) <= 0:
        raise ValueError('Quota value must be at least 1 character long.')

    if value == -1:
        return True

    if not re.fullmatch("[0-9]+[KMGTPEZY]?", value):
        raise ValueError('Improper quota format.')

    return True


def get_default_quota(in_bytes=False):
    config = get_config()
    default_quota = config['default_quota']

    check_quota_format(default_quota)

    if in_bytes:
        return to_bytes(default_quota)

    return default_quota


def database_in_config(database):
    config = get_config()
    return database in config['databases']


def get_quota(database):
    config = get_config()
    if database_in_config(database):
        return config['databases'][database]

    return get_default_quota()


def should_ignore(database):
    config = get_config()

    if not database_in_config(database):
        return False

    if config['databases'][database] == str(-1):
        return True

    return False


def to_bytes(value):
    check_quota_format(value)

    last = value[-1]

    num = int(re.match('[0-9]+', value).group(0))

    if last == 'K':
        multiplier = 1000
    elif last == 'M':
        multiplier = 1000 ** 2
    elif last == 'G':
        multiplier = 1000 ** 3
    elif last == 'T':
        multiplier = 1000 ** 4
    elif last == 'P':
        multiplier = 1000 ** 5
    elif last == 'E':
        multiplier = 1000 ** 6
    elif last == 'Z':
        multiplier = 1000 ** 7
    elif last == 'Y':
        multiplier = 1000 ** 8
    else:
        multiplier = 1

    return num * multiplier


def run():
    try:
        config = get_config()
        conn = mysql.connector.connect(**config['mysql'])

        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        cursor.close()

        for database in databases:
            database_name = database[0]
            if not should_ignore(database_name):
                used_space = 0

                cursor = conn.cursor()
                cursor.execute("SHOW TABLE STATUS FROM " + database_name)

                dli = cursor.column_names.index('Data_length')
                ili = cursor.column_names.index('Index_length')

                for row in cursor:
                    if not row[dli] is None:
                        used_space += int(row[dli])

                    if not row[ili] is None:
                        used_space += int(row[ili])

                cursor.close()

                # Database's used space is greater or equal to the database's quota
                # TODO: Disable create, insert, and update
                if used_space >= to_bytes(get_quota(database_name)):
                    print(database_name)
                else:
                    print(database_name) # TODO: If the usage is fine make sure to revert the restrictions on limited DBs
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Incorrect user and password combination")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)
    else:
        conn.close()


if __name__ == '__main__':
    run()
