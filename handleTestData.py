import pandas as pd

from utils import *

def build(host, port, user, password, database):
    connector = MySQL_processor(host, port, user, password)
    connector.cursor.execute('CREATE DATABASE IF NOT EXISTS %s' %(database))
    connector.cnx.close()

    connector = MySQL_processor(host, port, user, password, database)

    ### songs
    df = pd.read_csv('testData/1.csv')
    table = 'songs'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `song_id` varchar(255) NOT NULL,
        `song_length` int NULL,
        `genre_ids` varchar(255) NULL,
        `artist_name` varchar(255) NULL,
        `composer` varchar(255) NULL,
        `lyricist` varchar(255) NULL,
        `language` int NULL,
        PRIMARY KEY (`song_id`)
    ) ENGINE=InnoDB""" %(table))

    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`song_id`, `song_length`, `genre_ids`, `artist_name`, `composer`, `lyricist`, `language`) VALUES (%s) ON DUPLICATE KEY UPDATE song_id = `song_id`" %(table, '%s, %s, %s, %s, %s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    ### members
    df = pd.read_csv('testData/2.csv')
    table = 'members'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `msno` varchar(255) NOT NULL,
        `city` int NULL,
        `bd` int NULL,
        `gender` varchar(255) NULL,
        `registered_via` int NULL,
        `registration_init_time` varchar(255) NULL,
        `expiration_date` varchar(255) NULL,
        PRIMARY KEY (`msno`)
    ) ENGINE=InnoDB""" %(table))

    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`msno`,`city`,`bd`,`gender`,`registered_via`,`registration_init_time`,`expiration_date`) VALUES (%s) ON DUPLICATE KEY UPDATE msno = `msno`" %(table, '%s, %s, %s, %s, %s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    ### song_extra
    df = pd.read_csv('testData/3.csv')
    table = 'song_extra'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `song_id` varchar(255) NOT NULL,
        `name` varchar(255) NULL,
        `isrc` varchar(255) NULL,
        PRIMARY KEY (`song_id`)
    ) ENGINE=InnoDB""" %(table))

    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`song_id`,`name`,`isrc`) VALUES (%s) ON DUPLICATE KEY UPDATE song_id = `song_id`" %(table, '%s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    ### train
    table = 'train'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `msno` varchar(255) NOT NULL,
        `song_id` varchar(255) NOT NULL,
        `source_system_tab` varchar(255) NULL,
        `source_screen_name` varchar(255) NULL,
        `source_type` varchar(255) NULL,
        `target` int NULL,
        PRIMARY KEY (`msno`, `song_id`),
        FOREIGN KEY (`msno`) REFERENCES members(`msno`),
        FOREIGN KEY (`song_id`) REFERENCES songs(`song_id`),
        FOREIGN KEY (`song_id`) REFERENCES song_extra(`song_id`)    
    ) ENGINE=InnoDB""" %(table))

    df = pd.read_csv('testData/0.csv')
    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`msno`, `song_id`, `source_system_tab`, `source_screen_name`, `source_type`, `target`) VALUES (%s) ON DUPLICATE KEY UPDATE msno = `msno`" %(table, '%s, %s, %s, %s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    connector.cnx.close()

def drop(host, port, user, password, database):
    connector = MySQL_processor(host, port, user, password)
    connector.cursor.execute('DROP DATABASE %s' %(database))
    connector.cnx.close()

def main():
    # MySQL 連線資訊（改成自己 local 環境）
    host='127.0.0.1'
    port=3306
    user='root'
    password='test'
    database='test'

    build(host, port, user, password, database)
    # drop(host, port, user, password, database)   # drop database

if __name__ == '__main__':
    main()