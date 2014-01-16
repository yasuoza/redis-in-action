require 'redis'
require 'minitest/autorun'

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE          = 432
ARTICLES_PER_PAGE   = 25

def article_vote(redis, user, article)
  cutoff = Time.now.to_f - ONE_WEEK_IN_SECONDS
  # Check to see if the article can still be voted on
  if redis.zscore('time:', article) < cutoff
    return
  end

  article_id = article.split(':', 2).last
  if redis.sadd("voted:#{article_id}", user)
    redis.zincrby('score:', VOTE_SCORE, article)
    redis.hincrby(article, :votes, 1)
  end
end

def post_article(redis, user, title, link)
  article_id = redis.incr('article:')

  voted = "voted:#{article_id}"
  redis.sadd(voted, user)
  redis.expire(voted, ONE_WEEK_IN_SECONDS)

  now = Time.now.to_f
  article = "article:#{article_id}"
  redis.hmset(article, :title, title, :link, link, :poster, user, :time, now, :votes, 1)

  score = now + VOTE_SCORE
  redis.zadd('score:', score, article)
  redis.zadd('time:', now.to_s, article)

  article_id
end

def get_articles(redis, page, order='score:')
  start = (page -1) * ARTICLES_PER_PAGE
  last = start + ARTICLES_PER_PAGE - 1

  ids = redis.zrevrange(order, start, last)

  ids.map do |id|
    redis.hgetall(id)
  end
end

def add_remove_groups(redis, article_id, to_add=[], to_remove=[])
  article = "article:#{article_id}"

  to_add.each do |group|
    redis.sadd("group:#{group}", article)
  end

  to_remove.each do |group|
    redis.srem("group:#{group}", article)
  end
end

def get_group_articles(redis, group, page, order='score:')
  key = order + group

  unless redis.exists(key)
    redis.zinterstore(key, ["group:#{group}", order], aggregate: 'max')
    redis.expire(key, 60)
  end

  get_articles(redis, page, key)
end

class TestCh01 < MiniTest::Unit::TestCase
  def setup
    @redis = Redis.new(db: 15)
  end

  def test_article_functionality
    article_id = post_article(@redis, 'username', 'A title', 'http://www.google.com')
    assert article_id

    article = @redis.hgetall("article:#{article_id}")
    print "Its HASH looks like: "
    puts article
    assert article


    article_vote(@redis, 'other_user', "article:#{article_id}")
    print "We voted for the article, it now has votes: "
    v = @redis.hget("article:#{article_id}", 'votes')
    puts v
    assert(v.to_i > 1)


    print "The currently highest-scoring articles are: "
    articles = get_articles(@redis, 1)
    puts articles
    assert(article.count >= 1)


    add_remove_groups(@redis, article_id, ['new-group'])
    print "We added the article to a new group, other articles include: "
    articles = get_group_articles(@redis, 'new-group', 1)
    puts articles
    assert(articles.count >= 1)
  end
end
