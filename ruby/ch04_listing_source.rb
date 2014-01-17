require 'minitest/autorun'
require 'redis'
require 'securerandom'

def process_logs(redis, path, &callback)
  current_file, offset = redis.mget('progress:file',
                                    'progress:position')

  def update_progress(fname, offset)
    redis.multi do
      redis.mset('progress:file', fname,
                 'progress:position', offset)
    end
  end

  Dir.glob(File.join(path, '*')).sort.each do |file|
    if file < current_file
      next
    end

    if file != current_file
      offset = 0
    end

    f = File.open(file)
    f.each_line do |line|
      next unless f.lineno > offset
      callback(redis, line)
      offset = f.lineno

      if f.lineno == 1000
        update_progress(file, offset)
      end
    end
    update_progress(file, offset)
  end
end

def wait_for_sync(mredis, sredis)
  identifier = SecureRandom.uuid
  mredis.zadd('sync:wait', Time.now.to_f, identifier)

  until sredis.info['master_link_status'] == 'up'
    sleep(0.001)
  end

  until sredis.zscore('sync:wait', identifier)
    sleep(0.001)
  end

  deadline = Time.now.to_f + 1.01
  while Time.now.to_f < deadline
    if sredis.info['aof_pending_bio_fsync'] == 0
      break
    end
    sleep(0.001)
  end

  #Clean up our status and clean out older entries that may have been left there
  mredis.zrem('sync:wait', identifier)
  mredis.zremrangebyscore('sync:wait', 0, Time.now.to_f - 900)
end

# Ruby redis client does not raise `WatchError` defined in Python client,
# ruby client returns `nil`.
# So we need to fetch the multi response and use it to determine whether list_item succeeded.
def list_item(redis, item_id, seller_id, price)
  inventory = "inventory:#{seller_id}"
  item = "#{item_id}.#{seller_id}"
  end_time = Time.now.to_f + 5
  result = false
  while Time.now.to_f < end_time
    redis.watch(inventory)
    unless redis.sismember(inventory, item_id)
      redis.unwatch()
      return false
    end
    exec_result = redis.multi do
      redis.zadd("market:", price, item)
      redis.srem(inventory, item)
    end
    return exec_result if exec_result
  end
  return false
end

def purchase_item(redis, buyer_id, item_id, seller_id, lprice)
  buyer = "users:#{buyer_id}"
  seller = "users:#{seller_id}"
  item = "#{item_id}.#{seller_id}"
  intentory = "inventory:#{buyer_id}"
  end_time = Time.now.to_f + 10

  while Time.now.to_f < end_time
    redis.watch('market:', buyer)
    price = redis.zscore('market:', item).to_i
    funds = redis.hget(buyer, 'funds').to_i

    if price != lprice || price > funds
      redis.unwatch
      return false
    end

    exec_result = redis.multi do
      redis.hincrby(seller, 'funds', price.to_i)
      redis.hincrby(buyer, 'funds', -price.to_i)
      redis.sadd(intentory, item_id)
      redis.zrem('market:', item)
    end
    return exec_result if exec_result
  end

  return false
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

def update_token_pipeline(redis, token, user, item=nil)
  timestamp = Time.now.to_f

  redis.hset('login:', token, user)

  redis.zadd('recent:', timestamp, token)

  if item
    redis.pipelined do
      redis.zadd("viewed:#{token}", timestamp, item)
      # Keep latest 25 viewed histories
      redis.zremrangebyrank("viewed:#{token}", 0, -26)
      redis.zincrby('viewed:', -1, item)
    end
  end
end

def benchmark_update_token(redis, duration)
  %w(update_token update_token_pipeline).map(&:to_sym).each do |function|
    count = 0
    start = Time.now.to_f
    end_time = start + duration

    while Time.now.to_f < end_time
      count += 1
      __send__(function, redis, 'token', 'user', 'item')
    end
    delta = Time.now.to_f - start

    puts "#{function}, #{count}, #{delta}, #{count / delta}"
  end
end


class TestCh04 < Minitest::Unit::TestCase
  def setup
    @redis = Redis.new(db: 15)
  end

  def test_list_item
    puts "We need to set up just enough state so that a user can list an item"
    seller = 'userX'
    item = 'itemX'
    @redis.sadd('inventory:' + seller, item)
    i = @redis.smembers('inventory:' + seller)
    puts "The user's inventory has: #{i}"
    assert(i)

    puts "Listing the item..."
    l = list_item(@redis, item, seller, 10)
    puts "Listing the item succeeded? #{l}"
    assert(l)
    r = @redis.zrange('market:', 0, -1, withscores: true)
    print "The market contains: "
    p r
    assert(r)
    assert(r.any? { |_r| _r[0] == 'itemX.userX' })
  end

  def test_purchase_item
    test_list_item

    puts "We need to set up just enough state so a user can buy an item"
    buyer = 'userY'
    @redis.hset('users:userY', 'funds', 125)
    r = @redis.hgetall('users:userY')
    puts "The user has some money: #{r}"
    assert(r)
    assert(r['funds'])

    puts "Let's purchase an item"
    p = purchase_item(@redis, 'userY', 'itemX', 'userX', 10)
    puts "Purchasing an item succeeded? #{p}"
    assert(p)
    r = @redis.hgetall('users:userY')
    puts "Their money is now: #{r}"
    assert(r)
    i = @redis.smembers('inventory:' + buyer)
    puts "Their inventory is now: #{i}"
    assert(i)
    assert(i.include?('itemX'))
    assert_equal(@redis.zscore('market:', 'itemX.userX'), nil)
  end

  def test_benchmark_update_token
    benchmark_update_token(@redis, 5)
  end
end
