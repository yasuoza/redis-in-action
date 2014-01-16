require 'redis'

def update_token(redis, token, user, item=nil)
  timestamp = Time.now.to_f
  redis.hset('login:', token, user)
  redis.zadd('recent:', timestamp, token)
  if item
    key = "viewed:#{token}"
    redis.lrem(key, item)
    redis.rpush(key, item)
    # Keep latest 25 viewed histories
    redis.ltrim(key, -25, -1)
    redis.zincrby('viewed:', -1, item)
  end
end

def publisher(n)
  sleep(1)

  # Publisher
  redis = Redis.new(db: 15)

  (0..n-1).each do |i|
    redis.publish('channel', i)
    sleep(1)
  end
end

def run_pubsub
  Thread.new { publisher(3) }

  # Subscriber
  redis = Redis.new(db: 15)

  count = 0
  redis.subscribe('channel') do |on|
    on.subscribe do |channel, message|
      puts "channel: #{channel}, subscribe message: #{message}"
    end

    on.message do |channel, message|
      puts "channel: #{channel}, on message: #{message}"
      count += 1
      if count == 3
        redis.unsubscribe
      end
    end
  end
end

def notrans
  redis = Redis.new(db: 15)

  puts redis.incr('notrans:')
  sleep(0.1)
  redis.incrby('notrans:', -1)
end

"""
# <start id='simple-pipeline-notrans'/>
3.times do |i|
  Thread.new { notrans }
end
sleep(0.5)
"""

def trans
  redis = Redis.new(db: 15)

  res = redis.multi do
    redis.incr('notrans:')
    sleep(0.1)
    redis.incrby('notrans:', -1)
  end
  puts res[1]
end

"""
# <start id='simple-pipeline-trans'/>
3.times do |i|
  Thread.new { trans }
end
sleep(0.5)
"""

def article_vote(redis, user, article)
  cutoff = Time.now.to_f - ONE_WEEK_IN_SECONDS
  # Check to see if the article can still be voted on
  posted = redis.zscore('time:', article)
  if posted < cutoff
    return
  end

  article_id = article.split(':', 2).last
  res = redis.multi do
    redis.sadd("voted:#{article_id}", user)
    redis.expire("voted:#{article_id}", posted - cutoff)
  end

  if res.first
    redis.multi do
      redis.zincrby('score:', VOTE_SCORE, article)
      redis.hincrby(article, :votes, 1)
    end
  end
end
# If voted:#{article_id} is expired between ZSCORE and ZADD,
# SADD record will not expired and it will be a memory leak.


def get_articles(redis, page, order='score:')
  start = (page -1) * ARTICLES_PER_PAGE
  last = start + ARTICLES_PER_PAGE - 1

  ids = redis.zrevrange(order, start, last)

  redis.pipelined do
    ids.map do |id|
      redis.hgetall(id)
    end
  end
end

THIRTY_DAYS = 30*86400
def update_token(redis, token, user, item=nil)
  timestamp = Time.now.to_f

  # Keep session only 7 days
  redis.setex("login:#{token}", user, THIRTY_DAYS)

  key = "viewed:#{token}"
  if item
    redis.lrem(key, item)
    redis.rpush(key, item)
    redis.ltrim(key, -25, -1)
    redis.zincrby('viewed:', -1, item)
  end
  redis.expire(key, THIRTY_DAYS)
end

def add_to_cart(redis, session, item, count)
  if count <= 0
    redis.hdel("cart:#{session}", item)
  else
    redis.hset("cart:#{session}", item, count)
  end
  redis.expire("cart:#{session}", redis.ttl("login:#{token}"))
end
