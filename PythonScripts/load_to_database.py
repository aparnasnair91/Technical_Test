import csv, sqlite3
import string

con = sqlite3.connect("mysql.db")
cur = con.cursor()


def truncate_table(table: string):
    cur.execute("delete from "+table)
    con.commit()
    print("Truncated values for ", table)


def load_to_tags(table: string):
    with open('output/tags_data', 'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['lookup_url_num'], i['html_node'], i['html_tag'], i['html_left_sibling'],
                  i['html_right_sibling'], i['html_parent']) for i in dr]
        cur.executemany("INSERT INTO "+table+" (lookup_url_num, html_node,html_tag,html_left_sibling,"
                                             "html_right_sibling, html_parent) "
                                             "VALUES (?, ?, ?, ?, ?, ?);", to_db)
        con.commit()
        print("Inserted records to table -", table)
        print("___________________________________________")


def load_to_webpage(table: string):
    with open('output/webpage_data', 'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['lookup_url_num'], i['url'], i['source_url'], i['source_offset'],
                  i['source_length']) for i in dr]
        cur.executemany("INSERT INTO "+table+" (lookup_url_num, url,source_url,source_offset,source_length) "
                        "VALUES (?, ?, ?, ?, ?);", to_db)
        con.commit()
        print("Inserted records to table -", table)
        print("___________________________________________")


def load_to_crawl(table: string):
    with open('output/crawl_data', 'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['lookup_url_num'], i['url'], i['source_url'], i['source_offset'],
                  i['source_length'], i['html_node'], i['html_tag'], i['html_left_sibling'],
                  i['html_right_sibling'], i['html_parent']) for i in dr]
        cur.executemany("INSERT INTO "+table+" (lookup_url_num, url,source_url,source_offset,"
                                             "source_length,html_node,html_tag,html_left_sibling, "
                                             "html_right_sibling,html_parent) "
                                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
        con.commit()
        print("Inserted records to table -", table)
        print("___________________________________________")


truncate_table("tags")
load_to_tags("tags")

truncate_table("webpage")
load_to_webpage("webpage")

truncate_table("full_crawl_data")
load_to_crawl("full_crawl_data")

con.close()
