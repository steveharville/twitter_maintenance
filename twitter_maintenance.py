#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import twitter_util2
client_name,target_name=twitter_util2.get_client_target_names()
db_connection,db_cursor=twitter_util2.setup_database(client_name, target_name)
twt=twitter_util2.get_twitter_handle()
twitter_util2.load_client_tables(twt,db_connection,db_cursor,client_name)
twitter_util2.load_target_tables(twt,db_connection,db_cursor,target_name)
twitter_util2.summarize_current_stats(db_connection,db_cursor)
more_to_follow = True
more_to_unfollow = True
try:
    while more_to_follow:
        target_followers=twitter_util2.get_list_to_follow(db_cursor)
        friends_not_following=twitter_util2.get_list_to_unfollow(db_cursor)
        while more_to_unfollow and more_to_follow:
            more_to_unfollow,more_to_follow=twitter_util2.unfollow_follow(twt,db_connection,
                db_cursor,target_followers,friends_not_following)
        more_to_follow=twitter_util2.follow(twt,db_connection,db_cursor,target_followers)
except:
    print "Finishing up"

twitter_util2.summarize_current_stats(db_connection,db_cursor)
db_connection.commit()
db_connection.close()

