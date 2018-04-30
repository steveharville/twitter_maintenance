#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from twython import Twython,TwythonError
import datetime
import time 
import random
import math
import sqlite3

def get_twitter_handle():
    APP_KEY=           "secret_app_key"
    APP_SECRET=        "top_secret_app"
    OAUTH_TOKEN=       "secret_oauth_token"
    OAUTH_TOKEN_SECRET="top_secret_oauth"
    return Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
def get_followers(twitter_handle,username,next_cursor):
    g_followers=twitter_handle.get_followers_ids(screen_name=username,
                count=5000, stringify_ids="false", cursor=next_cursor)
    return g_followers
def get_friends(twitter_handle,username,next_cursor):
    g_friends=twitter_handle.get_friends_ids(screen_name=username,
                count=5000, stringify_ids="false", cursor=next_cursor)
    return g_friends
def check_limits(twitter_handle):    
    print "In check_limits"
    Limit=twitter_handle.get_lastfunction_header("x-rate-limit-limit")
    Remaining=twitter_handle.get_lastfunction_header("x-rate-limit-remaining")
    epoch_time = int(time.time())
    min_to_reset=(int(twitter_handle.get_lastfunction_header("x-rate-limit-reset"))
            - epoch_time) / 60
    print "Limit="+Limit
    print "Remaining="+Remaining
    print "Min_to_reset="+str(min_to_reset)
    return Remaining,min_to_reset
def wait_for_reset(twitter_handle):
    print "In wait for reset - calling check_limits"
    time.sleep(60)
    remaining_calls,min_to_wait=check_limits(twitter_handle)
    if (int(remaining_calls) == 0  ):
        print "Remaining calls=" + remaining_calls
        print "Minutes_to_wait=" + str(min_to_wait)
        time.sleep(60*min_to_wait+60)
    return
def wait_random_time():
    print "Waiting " 
    minimum=60
    maximum=600
    rand=math.floor(random.random() * (maximum - minimum + 1) + minimum)
    print rand
    time.sleep(rand)
def get_client_target_names():
    client_name='client_twitter_name'
    target_name='twitter_account_with_content_like_client_the_more_followers_the_better'
    return client_name,target_name
def setup_database(client_name,target_name):
    db_name=client_name + "_" + target_name + ".sqlite"
    conn=sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("""create table if not exists followers (
    follower_seq    integer primary key, 
    follower_id     integer unique not null) 
    """)
    c.execute("""create table if not exists current_followers (
    curfollower_seq    integer primary key, 
    curfollower_id     integer unique not null) 
    """)
    c.execute("""create table if not exists friends (
    friend_seq  integer primary key,
    friend_id   integer unique  not null )
    """)
    c.execute("""create table if not exists target_followers (
    targetfollow_seq  integer primary key,
    targetfollow_id   integer unique  not null) 
    """)
    c.execute("""create table if not exists friends_I_unfollowed (
    friendun_seq  integer primary key,
    friendun_id   integer unique  not null) 
    """)
    c.execute("""create table if not exists requested_to_follow (
    request_seq  integer primary key,
    request_id   integer unique  not null) 
    """)
    c.execute("""create table if not exists follow_follower_summary (
    fol_seq           integer primary key,
    recorded          date default current_timestamp,
    follower_count    integer not null,
    unfollow_count    integer not null,
    friend_count      integer not null) 
    """)
    return conn,c
def load_client_tables(twt,db_connection,db_cursor,client_name):
    followers = []
    friends = []
    next_cursor = -1
    while(next_cursor):
        l_followers = get_followers(twt,client_name,next_cursor)
        print ("Get_followers contains " + str(len(l_followers["ids"]))
        + " follower ids " )
        print "next_cursor contains " + str(next_cursor) 
        wait_for_reset(twt)
        followers.append(l_followers["ids"])
        next_cursor = l_followers["next_cursor"]
    all_followers=sum(followers,[])
    follower_tuples=[(i,) for i in all_followers]
    db_cursor.executemany("""insert or replace into followers (follower_id) 
                values(?)""" , (follower_tuples))
    db_cursor.execute("""delete from current_followers""")
    db_cursor.executemany("""insert into current_followers (curfollower_id) 
                values(?)""" , (follower_tuples))
    db_connection.commit()
    print "Completed loading historical and current followers"
    next_cursor = -1
    while(next_cursor):
        l_friends = get_friends(twt,client_name,next_cursor)
        print "Get_friends contains " + str(len(l_friends["ids"])) + " friend IDs"
        print "next_cursor contains " + str(next_cursor)
        wait_for_reset(twt)
        friends.append(l_friends["ids"])
        next_cursor = l_friends["next_cursor"]
    all_friends=sum(friends,[])
    friend_tuples=[(i,) for i in all_friends]
    db_cursor.executemany("""insert or replace into friends (friend_id) 
                values(?)""" , (friend_tuples))
    db_connection.commit()
    print "Completed loading friends"
    return
def load_target_tables(twt,db_connection,db_cursor,target_name):
    followers = []
    next_cursor = -1
    while(next_cursor):
        l_followers = get_followers(twt,target_name,next_cursor)
        print ("Get_followers contains " + str(len(l_followers["ids"])) 
        + " follower ids " )
        print "next_cursor contains " + str(next_cursor) 
        wait_for_reset(twt)
        followers.append(l_followers["ids"])
        next_cursor = l_followers["next_cursor"]
    all_followers=sum(followers,[])
    follower_tuples=[(i,) for i in all_followers]
    db_cursor.executemany("""insert or replace into target_followers 
      (targetfollow_id) values(?)""" , (follower_tuples))
    db_connection.commit()
    return
def summarize_current_stats(db_connection,db_cursor):
    db_cursor.execute("""select count(*) from followers""")
    follower_count=db_cursor.fetchone()[0] 
    print "Followers "+ str(follower_count)
    db_cursor.execute("""select count(*) from friends_I_unfollowed""")
    beginning_unfollow_count=db_cursor.fetchone()[0]
    print "Unfollowed " + str(beginning_unfollow_count)
    db_cursor.execute("""select count(*) from friends""")
    beginning_friend_count=db_cursor.fetchone()[0] - beginning_unfollow_count
    print "Following " + str(beginning_friend_count)
    current_friend_count=beginning_friend_count
    beginning_follower_to_following_ratio=(follower_count / (beginning_friend_count*1.0+.0001))
    print "Follower/following ratio " + str(beginning_follower_to_following_ratio)
    db_cursor.execute("""
    insert into follow_follower_summary (follower_count,unfollow_count,friend_count)
    values (""" 
     + str(follower_count) + """,""" 
     + str(beginning_unfollow_count) + """,""" 
     + str(beginning_friend_count) + """)
    """)
    db_connection.commit()
    return
def get_list_to_follow(db_cursor):
    db_cursor.execute("""with
     target_followers_not_following_me as (
         select targetfollow_id from target_followers
          except
         select follower_id from followers
          except
         select request_id from requested_to_follow 
     ),
     friends_not_following_me as (
        select  friend_id from friends 
        except
        select follower_id from followers,friends 
            where follower_id = friend_id
    ),
    already_unfollowed as (
        select friendun_id from friends_I_unfollowed
    )
        select targetfollow_id from target_followers_not_following_me
        except 
        select friend_id from friends_not_following_me
        except
        select friendun_id from already_unfollowed
    """)
    target_followers=db_cursor.fetchall() 
    return target_followers
def follow(twt,db_connection,db_cursor,target_followers):
    if target_followers:
        next_target_follower=target_followers.pop(0)
        print "Now following " + str(next_target_follower[0])
        try:
            twt.create_friendship(user_id=next_target_follower[0])
        except TwythonError as e:
            print "Error Message : " + e.msg
            if  " unable to follow more " in e.msg or "Invalid or expired token"  in  e.msg or "spam and other malicious activity" in e.msg:
                raise
            else:
                pass
        db_cursor.execute("""
        insert into requested_to_follow (request_id) values(""" 
        + str(next_target_follower[0]) + """)""") 
        db_connection.commit()
        wait_random_time()
        return next_target_follower 
    else:
        return False
def get_list_to_unfollow(db_cursor):
    db_cursor.execute("""
    with
    were_following_not_now as (
     select follower_id from followers
     except
     select curfollower_id from current_followers
     except
     select friendun_id from friends_I_unfollowed),
    friends_not_following_me as (
     select  friend_id from friends 
     except
     select follower_id from followers,friends 
     where follower_id = friend_id),
    already_unfollowed as (
     select friendun_id from friends_I_unfollowed)
    select follower_id from were_following_not_now
    union
    select friend_id from friends_not_following_me
    except
    select friendun_id from already_unfollowed
     """)
    friends_not_following=db_cursor.fetchall()
    return friends_not_following
def unfollow_follow(twt,db_connection,db_cursor,target_followers,friends_not_following):
    more_to_unfollow=True
    more_to_follow=True
    while more_to_unfollow:
        for friendnot in friends_not_following:
            print "Un following : " + str(friendnot[0])
            db_cursor.execute("""select count(*) from friends_I_unfollowed
                    where friendun_id=""" 
                + str(friendnot[0]))
            already_unfollowed=db_cursor.fetchone()
            if ( already_unfollowed[0] > 0 ) :
                print "Already unfollowed   - skipping"
                continue
            try:
                twt.destroy_friendship(user_id=friendnot[0])    
                db_cursor.execute("""
                insert into friends_I_unfollowed (friendun_id) values(""" + str(friendnot[0]) 
                + """)""") 
                db_connection.commit()
                wait_random_time()
                more_to_follow=follow(twt,db_connection,db_cursor,target_followers)
            except TwythonError as e:
                print "Error Message : " + e.msg
                if  " unable to follow more " in e.msg or "Invalid or expired token"  in  e.msg or "spam and other malicious activity" in e.msg: 
                    raise
                else:
                    pass
        more_to_unfollow=False
    return more_to_unfollow,more_to_follow
