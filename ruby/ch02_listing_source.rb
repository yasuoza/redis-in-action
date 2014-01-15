require 'minitest/autorun'
require 'securerandom'
require 'uri'
require 'cgi'
require 'json'
require 'redis'

def check_token(redis, token)
  redis.hget('login:', token)
end

def update_token(redis, token, user, item=nil)
  timestamp = Time.now.to_f

  redis.hset('login:', token, user)

  redis.zadd('recent:', timestamp, token)

  if item
    redis.zadd("viewed:#{token}", timestamp, item)
    # Keep latest 25 viewed histories
    redis.zremrangebyrank("viewed:#{token}", 0, -26)
    redis.zincrby('viewed:', -1, item)
  end
end

QUIT  = false
LIMIT = 10_000_000

def clean_sessions(redis)
  until QUIT
    size = redis.zcard('recent:')
    if size <= LIMIT
      sleep(1)
      next
    end

    end_index = [size - LIMIT, 100].min
    tokens = redis.zrange('recent:', 0, end_index - 1)

    session_keys = tokens.map { |token| "viewd:#{token}" }

    redis.del(session_keys)
    redis.hdel('login:', tokens)
    redis.zrem('recent:', tokens)
  end
end

def add_to_cart(redis, session, item, count)
  if count <= 0
    redis.hdel("cart:#{session}", item)
  else
    redis.hset("cart:#{session}", item, count)
  end
end

def clean_full_sessions(redis)
  until QUIT
    size = redis.zcard('recent:')
    if size <= LIMIT
      sleep(1)
      next
    end

    end_index = [size - LIMIT, 100].min
    tokens = redis.zrange('recent:', 0, end_index - 1)

    session_keys = []
    tokens.each do |token|
      session_keys << "viewd:#{token}"
      session_keys << "cart:#{token}"
    end

    redis.del(session_keys)
    redis.hdel('login:', tokens)
    redis.zrem('recent:', tokens)
  end
end

def cache_request(redis, request)
  unless can_cache(redis, request)
    return yield request if block_given?
  end

  page_key = "cache:#{hash_request(request)}"
  content = redis.get(page_key)

  unless content
    content = yield request if block_given?
    redis.setex(page_key, 300, content)
  end

  content
end

def schedule_row_cache(redis, row_id, delay)
  # Set the delay for the item first
  redis.zadd('delay:', delay, row_id)

  # Schedule the item to be cached now
  redis.zadd('schedule:', Time.now.to_f, row_id)
end

def cache_rows(redis)
  until QUIT
    targets = redis.zrange('schedule:', 0, 0, withscores: true)
    now = Time.now.to_f
    if targets.nil? || targets.empty? || targets[0][1] > now
      sleep(0.05)
      next
    end

    row_id = targets[0][0]
    delay = redis.zscore('delay:', row_id)

    # The item shouldn't be cached anymore, remove it from the cache
    if delay <= 0
      redis.zrem('delay:', row_id)
      redis.zrem('schedule:', row_id)
      redis.del("inv:#{row_id}")
      next
    end

    row = Inventory.get(row_id)
    redis.zadd('schedule:', now + delay, row_id)
    redis.set("inv:#{row_id}", JSON.dump(row.to_h))
  end
end

def rescale_viewed(redis)
  until QUIT
    redis.zremrangebyrank('viewed:', 20_000, -1)

    # Rescale all counts to be 1/2 of what they were before
    redis.zinterstore('viewed:', 'viewed' => 0.5)

    sleep(300)
  end
end

def can_cache(redis, request)
  item_id = extract_item_id(request)

  #Check whether the page can be statically cached, and whether this is an item page
  if !item_id || is_dynamic(request)
    return false
  end

  rank = redis.zrank('viewed:', item_id)

  #Return whether the item has a high enough view count to be cached
  rank && rank < 10_000
end

#--------------- Below this line are helpers to test the code ----------------
def extract_item_id(request)
  query = URI.parse(request).query
  !query.nil? && !query.empty? && CGI.parse(query)['item']
end

def is_dynamic(request)
  query = URI.parse(request).query
  !query.nil? && !query.empty? && CGI.parse(query)['_'].any?
end

def hash_request(request)
  request.hash
end

class Inventory < Struct.new(:id)
  def self.get(id)
    new(id)
  end

  def to_h
    {id: id, data: 'data to cache...', cached: Time.now.to_f}
  end
end


class TestCh02 < MiniTest::Unit::TestCase
  def setup
    @redis = Redis.new(db: 15)
  end

  def teardown
    # Revert constants
    Object.send(:remove_const, 'LIMIT')
    Object.const_set('LIMIT', 10_000_000)
    Object.send(:remove_const, 'QUIT')
    Object.const_set('QUIT', false)
  end

  def test_login_cookies
    token = SecureRandom.uuid

    update_token(@redis, token, 'username', 'itemX')
    print "We just logged-in/updated token: "
    puts token
    print "For user: "
    puts 'username'

    puts "What username do we get when we look-up that token?"
    r = check_token(@redis, token)
    puts r
    assert(r)

    puts "Let's drop the maximum number of cookies to 0 to clean them out"
    puts "We will start a thread to do the cleaning, while we stop it later"

    Object.send(:remove_const, 'LIMIT')
    Object.const_set('LIMIT', 0)
    t = Thread.new(@redis) { |redis| clean_sessions(redis) }
    sleep(1)
    Object.send(:remove_const, 'QUIT')
    Object.const_set('QUIT', true)
    sleep(2)
    if t.alive?
      raise Exception.new "The clean sessions thread is still alive?!?!"
    end

    s = @redis.hlen('login:')
    puts "The current number of sessions still available is: #{s}"
    assert_equal(s, 0)
  end

  def test_shopping_cart_cookies
    token = SecureRandom.uuid

    puts "We'll refresh our session..."
    update_token(@redis, token, 'username', 'itemX')
    puts "And add an item to the shopping cart"
    add_to_cart(@redis, token, "itemY", 3)
    r = @redis.hgetall("cart:#{token}")
    puts "Our shopping cart currently has: #{r}"
    assert(r.length >= 1)

    puts "Let's clean out our sessions and carts"
    Object.send(:remove_const, 'LIMIT')
    Object.const_set('LIMIT', 0)
    t = Thread.new(@redis) { |redis| clean_full_sessions(redis) }
    sleep(1)
    Object.send(:remove_const, 'QUIT')
    Object.const_set('QUIT', true)
    sleep(2)
    t.join
    if t.alive?
      raise Exception.new "The clean sessions thread is still alive?!?!"
    end

    r = @redis.hgetall('cart:' + token)
    puts "Our shopping cart now contains:#{r}"
    assert(r.empty?)
  end

  def test_cache_request
    token = SecureRandom.uuid

    update_token(@redis, token, 'username', 'itemX')
    url = 'http://test.com/?item=itemX'
    puts "We are going to cache a simple request against #{url}"
    result = cache_request(@redis, url) do |request|
      "content for #{request}"
    end
    puts "We got initial content: #{result}"

    assert(result)

    puts "To test that we've cached the request, we'll pass a bad callback"
    result2 = cache_request(@redis, url)
    puts "We ended up getting the same response! #{result2}"

    assert_equal(result, result2)

    assert(!can_cache(@redis, 'http://test.com'))
    assert(!can_cache(@redis, 'http://test.com/?item=itemX&_=123456'))
  end

  def test_cache_rows
    puts "First, let's schedule caching of itemX every 5 seconds"
    schedule_row_cache(@redis, 'itemX', 5)
    puts "Our schedule looks like:"
    s = @redis.zrange('schedule:', 0, -1, withscores: true)
    p s
    assert(s)


    puts "We'll start a caching thread that will cache the data..."
    t = Thread.new(@redis) { |redis| cache_rows(redis) }
    sleep(1)
    puts "Our cached data looks like:"
    r = @redis.get('inv:itemX')
    puts r
    assert(r)

    puts "We'll check again in 5 seconds..."
    sleep(5)
    puts "Notice that the data has changed..."
    r2 = @redis.get('inv:itemX')
    assert(r2)
    puts r2
    assert(r != r2)


    puts "Let's force un-caching"
    schedule_row_cache(@redis, 'itemX', -1)
    sleep(1)
    r = @redis.get('inv:itemX')
    puts "The cache was cleared? #{!r}"
    assert(!r)

    Object.send(:remove_const, 'QUIT')
    Object.const_set('QUIT', true)
    sleep(2)
    if t.alive?
      raise Exception.new "The clean sessions thread is still alive?!?!"
    end
  end
end

