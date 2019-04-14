from mysql.connector import connect as mysql_connect
from mysql.connector import Error as mysql_error
from mysql.connector import errorcode as mysql_errorcode
from json import load as json_load
from re import match, fullmatch
from os.path import isfile
from time import sleep

DEFAULT_CONFIG = 'config.json'


def get_json_config(file: str=DEFAULT_CONFIG):
    if not isinstance(file, str):
        raise TypeError('Optional file parameter must be a string.')

    if not isfile(file):
        raise FileNotFoundError('That config file does not exist!')

    return json_load(open(file))


def check_quota_format(value: str) -> bool:
    if not isinstance(value, str):
        raise TypeError('Value parameter must be a string.')

    if len(value) <= 0:
        raise ValueError('Quota value must be at least 1 character long.')

    if value == -1:
        return True

    if not fullmatch("[0-9]+[KMGTPEZY]?", value):
        raise ValueError('Improper quota format.')

    return True


def get_default_quota(in_bytes: bool=False) -> str:
    if not isinstance(in_bytes, bool):
        raise TypeError('Optional in_bytes parameter must be a boolean.')

    config = get_json_config()
    default_quota = config['default_quota']

    check_quota_format(default_quota)

    if in_bytes:
        return str(to_bytes(default_quota))

    return default_quota


def database_in_config(database: str) -> bool:
    if not isinstance(database, str):
        raise TypeError('Database parameter must be a string.')

    config = get_json_config()
    return database in config['databases']


def get_quota(database: str) -> str:
    if not isinstance(database, str):
        raise TypeError('Database parameter must be a string.')

    config = get_json_config()
    if database_in_config(database):
        return config['databases'][database]

    return get_default_quota()


def should_ignore(database: str) -> bool:
    if not isinstance(database, str):
        raise TypeError('Database parameter must be a string.')

    config = get_json_config()

    if not database_in_config(database):
        return False

    if config['databases'][database] == str(-1):
        return True

    return False


def to_bytes(value: str) -> int:
    if not isinstance(value, str):
        raise TypeError('Value parameter must be a string.')

    check_quota_format(value)

    last = value[-1]

    num = int(match('[0-9]+', value).group(0))

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


def get_databases(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    cursor.close()

    return databases


def get_db_usage(conn, database: str) -> int:
    if not isinstance(database, str):
        raise TypeError('Database parameter must be a string.')

    used_space = 0

    cursor = conn.cursor()
    cursor.execute("SHOW TABLE STATUS FROM " + database)

    dli = cursor.column_names.index('Data_length')
    ili = cursor.column_names.index('Index_length')

    for row in cursor:
        if not row[dli] is None:
            used_space += int(row[dli])

        if not row[ili] is None:
            used_space += int(row[ili])

    cursor.close()

    return used_space


def limit(conn, database: str) -> None:
    print('Limiting ' + database)
    conn.config(database='mysql')
    conn.reconnect()

    cursor = conn.cursor()
    for user in db_users(conn, database):
        cursor.execute("REVOKE create,insert,update ON " + database + ".* FROM '" + user['user'] + "'@'" + user['host'] + "';")
        kill_user(conn, database, user['user'])

    cursor.close()

    conn.config(database=None)
    conn.reconnect()


def unlimit(conn, database: str) -> None:
    print('Unlimiting ' + database)
    conn.config(database='mysql')
    conn.reconnect()

    cursor = conn.cursor()
    for user in db_users(conn, database):
        cursor.execute("GRANT create,insert,update ON " + database + ".* TO '" + user['user'] + "'@'" + user['host'] + "';")
        kill_user(conn, database, user['user'])

    cursor.close()

    conn.config(database=None)
    conn.reconnect()


def is_limited(conn, database: str) -> bool:
    conn.config(database='mysql')
    conn.reconnect()

    cursor = conn.cursor()
    cursor.execute("SELECT Insert_priv,Create_priv,Update_priv FROM db WHERE Db='" + database + "'")

    limited = False
    for row in cursor.fetchall():
        for column in row:
            if column == 'N':
                limited = True
                break

        if limited:
            break

    cursor.close()

    conn.config(database=None)
    conn.reconnect()

    return limited


def db_users(conn, database: str) -> list:
    users = []

    conn.config(database='mysql')
    conn.reconnect()

    cursor = conn.cursor()
    cursor.execute("SELECT `user`,`host` FROM db WHERE `db` = %s;", (database,))

    for row in cursor.fetchall():
        user = row[0]
        host = row[1]
        users.append({'user': user, 'host': host})

    cursor.close()

    conn.config(database=None)
    conn.reconnect()

    return users


def kill_user(conn, database: str, user: str) -> None:
    conn.config(database='information_schema')
    conn.reconnect()

    cursor = conn.cursor()
    cursor.execute("SELECT `id` FROM processlist WHERE `db` = %s AND `user` = %s;", (database, user))

    for row in cursor.fetchall():
        pid = str(row[0])
        print("Killing MySQL user PID: " + pid)
        cursor.execute("KILL " + pid + ";")

    cursor.close()

    conn.config(database=None)
    conn.reconnect()


def run() -> None:
    try:
        config = get_json_config()
        conn = mysql_connect(**config['mysql'])

        databases = get_databases(conn)

        for database in databases:
            database_name = database[0]
            if not should_ignore(database_name):
                used_space = get_db_usage(conn, database_name)

                # Database's used space is greater or equal to the database's quota
                if used_space >= to_bytes(get_quota(database_name)) and not is_limited(conn, database_name):
                    limit(conn, database_name)
                elif used_space < to_bytes(get_quota(database_name)) and is_limited(conn, database_name):
                    unlimit(conn, database_name)
    except mysql_error as e:
        if e.errno == mysql_errorcode.ER_ACCESS_DENIED_ERROR:
            print("Incorrect user and password combination")
        elif e.errno == mysql_errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)
    else:
        conn.close()


if __name__ == '__main__':
    while True:
        run()
        sleep(get_json_config()['check_frequency'])  # wait X seconds before checking again
