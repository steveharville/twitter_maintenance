#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
# copyright 2017 Steve Harville
import twitter_util2
client_name,target_name=twitter_util2.get_client_target_names()
db_connection,db_cursor=twitter_util2.setup_database(client_name, target_name)
twt=twitter_util2.get_twitter_handle()
twitter_util2.load_client_tables(twt,db_connection,db_cursor,client_name)
twitter_util2.load_target_tables(twt,db_connection,db_cursor,target_name)
twitter_util2.summarize_current_stats(db_connection,db_cursor)
db_connection.commit()
db_connection.close()
