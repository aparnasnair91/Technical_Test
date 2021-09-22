import os
import pandas as pd
import sqlite3

from utils import load_single_warc_record, get_html_extracts

CRAWL_LOC = os.path.expanduser(os.path.join('~', 'PycharmProjects', 'ExtractAndManipulate', 'output',
                                            'crawl_data'))
WEBPAGE_LOC = os.path.expanduser(os.path.join('~', 'PycharmProjects', 'ExtractAndManipulate', 'output',
                                              'webpage_data'))
TAGS_LOC = os.path.expanduser(os.path.join('~', 'PycharmProjects', 'ExtractAndManipulate', 'output',
                                           'tags_data'))

try:
    sqliteConnection = sqlite3.connect('mysql.db')
    print("Successfully Connected to SQLite Database")

    select_Query = '''select url, source_url, source_offset, source_length
              from news_manual_edit
              where collected_by like '%CommonCrawl%'
              order by random()
              '''
    df_nme = pd.read_sql(select_Query, sqliteConnection)

    # Iterate and extract data for each url
    full_extracts = []
    tags_extracts = []
    webpage_extracts = []
    for index in range(0, len(df_nme), 1):
        lookup_url_num = index + 1
        html_lookup = df_nme.iloc[index]
        url = html_lookup["url"]
        source_url = html_lookup["source_url"]
        source_offset = html_lookup["source_offset"]
        source_length = html_lookup["source_length"]

        webpage_extracts_tmp = [lookup_url_num, url, source_url, source_offset,
                                source_length]
        webpage_extracts.append(webpage_extracts_tmp)

        html = load_single_warc_record(source=source_url,
                                       offset=source_offset,
                                       length=int(source_length))

        for extracts in get_html_extracts(html):
            node = extracts[0]
            tag = extracts[1]
            left_sibling = extracts[2]
            right_sibling = extracts[3]
            parent = extracts[4]

            full_extracts_tmp = [lookup_url_num, url, source_url, source_offset, source_length,
                                 node, tag, left_sibling, right_sibling, parent]
            tags_extracts_tmp = [lookup_url_num, node, tag, left_sibling, right_sibling, parent]

            tags_extracts.append(tags_extracts_tmp)
            full_extracts.append(full_extracts_tmp)

    full_df = pd.DataFrame(data=full_extracts, columns=['lookup_url_num', 'url', 'source_url',
                                                        'source_offset', 'source_length',
                                                        'html_node', 'html_tag', 'html_left_sibling',
                                                        'html_right_sibling',
                                                        'html_parent']).drop_duplicates(subset=None,
                                                                                        keep='first',
                                                                                        inplace=False)
    tags_df = pd.DataFrame(data=tags_extracts, columns=['lookup_url_num', 'html_node', 'html_tag',
                                                        'html_left_sibling', 'html_right_sibling',
                                                        'html_parent']).drop_duplicates(subset=None,
                                                                                        keep='first',
                                                                                        inplace=False)
    webpage_df = pd.DataFrame(data=webpage_extracts, columns=['lookup_url_num', 'url', 'source_url',
                                                              'source_offset',
                                                              'source_length']).drop_duplicates(subset=None,
                                                                                                keep='first',
                                                                                                inplace=False)

    tags_df.to_csv(TAGS_LOC, index=False)
    webpage_df.to_csv(WEBPAGE_LOC, index=False)
    full_df.to_csv(CRAWL_LOC, index=False)
    print("Data extracted and saved as CSV files")

except sqlite3.Error as error:
    print("Error while connecting to sqlite", error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print("The SQLite connection is closed")
