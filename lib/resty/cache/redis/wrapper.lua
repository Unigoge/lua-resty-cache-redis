local _M = {
  _VERSION = "0.1"
}

local redis = require "resty.redis"
local job = require "job"
local common = require "resty.cache.common"

local tinsert = table.insert
local unpack, assert, type, xpcall = unpack, assert, type, xpcall
local pairs, ipairs, next = pairs, ipairs, next
local traceback = debug.traceback

local update_time = ngx.update_time
local now, time = ngx.now, ngx.time

local ngx_null = ngx.null

local ngx_log = ngx.log
local ERR, WARN, INFO = ngx.ERR, ngx.WARN, ngx.INFO

local foreachi, foreach, foreach_v = common.foreachi, common.foreach, common.foreach_v

local random = math.random
local gsub, format = string.gsub, string.format
local uuid_template ='xxxxxxxx-xxxx-' .. ngx.worker.pid() .. '-yxxx-xxxxxxxxxxxx'

local function uuid()
  return gsub(uuid_template, '[xy]', function (c)
    local v = (c == 'x') and random(0, 0xf) or random(8, 0xb)
    return format('%x', v)
  end)
end

--[[ public api --]]

local redis_class = {}

function _M.new(opts)
  assert(opts, "opts required")
  return setmetatable(opts, {
    __index = redis_class
  })
end

--[[ redis class --]]

function redis_class:clone()
  local opts = {}
  foreach(self, function(k,v)
    opts[k] = v
  end)
  return setmetatable(opts, {
    __index = redis_class
  })
end

function redis_class:handle(f, upstream, ...)
  local red

  red = redis:new()
  red:set_timeout(self.timeout)

  assert(red:connect(upstream))

  local ok, response, err = xpcall(f, function(err)
    ngx_log(ERR, traceback())
    return err
  end, red, ...)

  if ok then
    red:set_keepalive(self.idle * 1000, self.pool_size)
  else
    red:close()
    error(response)
  end

  return response, err
end

function redis_class:sscan(red, key, pattern, callback)
  local keys = {}
  local cursor = "0"
  callback = callback or function(key) tinsert(keys, key) end
  repeat
    local t = assert(red:sscan(key, cursor, "count", 1000, "match", pattern))
    local keys_part, err
    cursor, keys_part, err = unpack(t)
    assert(not err, err)
    if next(keys_part) then
      foreach_v(keys_part, callback)
    end
  until cursor == "0"
  return keys
end

function redis_class:zscan(red, key, pattern, callback)
  local keys = {}
  local cursor = "0"
  callback = callback or function(key, score) tinsert(keys, { key = key, score = score } ) end
  repeat
    local t = assert(red:zscan(key, cursor, "count", 1000, "match", pattern))
    local keys_part, err
    cursor, keys_part, err = unpack(t)
    assert(not err, err)
    if next(keys_part) then
      foreach(red:array_to_hash(keys_part), callback)
    end
  until cursor == "0"
  return keys
end

function redis_class:scan(red, pattern, callback)
  local keys = {}
  local cursor = "0"
  callback = callback or function(key) tinsert(keys, key) end
  repeat
    local t = assert(red:scan(cursor, "count", 1000, "match", pattern))
    local keys_part, err
    cursor, keys_part, err = unpack(t)
    assert(not err, err)
    if next(keys_part) then
      foreach_v(keys_part, callback)
    end
  until cursor == "0"
  return keys
end

function redis_class:cleanup_keys_job(cache_name, cache_id)
  local cleanup = function(red)
    local start = now()
    local keys, err = red:zrangebyscore(cache_id .. ":keys", 0, start, "LIMIT", 0, 1000)
    if not keys then
      ngx_log(WARN, err)
      return 0
    end

    if #keys == 0 then
      -- nothing to clean
      return 0
    end

    local count, err = red:zcard(cache_id .. ":primary")
    if not count then
      ngx_log(WARN, err)
      return 0
    end

    red:init_pipeline()

    for j=1,#keys
    do
      local key = keys[j]
      if count ~= 0 then
        red:zrem(cache_id .. ":primary", key)
      end
      red:del(key)
      red:zrem(cache_id .. ":keys", key)
    end

    local ok, err = red:commit_pipeline()
    if not ok then
      ngx_log(WARN, err)
      return 0
    end

    if #keys ~= 0 then
      ngx_log(INFO, cache_name, " expired=", #keys, " at ", now() - start, " seconds")
    end

    return #keys
  end

  job.new("cleanup " .. cache_name, function()
    self:handle(function(red)
      local lock = self:create_lock(cache_id, 10)
      if lock:aquire() then
        repeat
          local count = cleanup(red)
        until count == 0 or not lock:prolong() or ngx.worker.exiting()
      end
    end, self.redis_rw)
    return true
  end, 10):run()
end

function redis_class:purge_keys(cache_name, cache_id)
  local purge_data = function(red, key)
    local count = 0
    local cursor = "0"

    repeat
      local t = assert(red:zscan(key, cursor, "count", 1000, "match", "*"))
      local keys, err
      cursor, keys, err = unpack(t)
      assert(not err, err)
      if next(keys) then
        keys = red:array_to_hash(keys)
        -- collect index keys
        local index_keys = {}
        foreach(keys, function(k)
          k = cache_id .. ":" .. k
          local ikeys
          ikeys = assert(self:zscan(red, k .. ":i", "*"))
          local index_keys_k = {}
          index_keys[k] = index_keys_k
          foreachi(ikeys, function(ikey)
            tinsert(index_keys_k, ikey.key)
          end)
        end)
        red:init_pipeline()
        -- flush data
        foreach(keys, function(k)
          red:multi()
          -- flush pk
          red:zrem(key, k)
          -- flush key
          k = cache_id .. ":" .. k
          red:del(k)
          -- flush index keys
          foreachi(index_keys[k], function(ikey)
            red:del(cache_id .. ":" .. ikey)
          end)
          -- flush key index references
          red:del(k .. ":i")
          red:exec()
          count = count + 1
        end)
        assert(red:commit_pipeline())
      end
    until cursor == "0"

    assert(red:del(key))

    return count
  end

  local purge = function(red)
    local count = 0

    ngx_log(INFO, cache_name .. "_purge() begin")

    repeat
      local renamed, err = red:renamenx(cache_id .. ":keys", cache_id .. ":purge")
      assert(renamed or err:match("no such key"), err)
      count = count + purge_data(red, cache_id .. ":purge")
    until renamed == 1 or err:match("no such key")

    ngx_log(INFO, cache_name .. "_purge() end, count=", count)

    return true
  end

  return self:handle(purge, self.redis_rw)
end

function redis_class:rw_socket()
  return self.redis_rw
end

function redis_class:ro_socket()
  return self.redis_ro
end

--[[ distributed lock --]]

local lock_class = {}

function redis_class:create_lock(name, period)
  local t = {
    last_prolong_time = 0,
    name = name,
    period = period,
    id = uuid(),
    red = self
  }
  return setmetatable(t, { __index = lock_class })
end

local scripts = {
  aquire = [[
    local key, id, period = KEYS[1], KEYS[2], KEYS[3]
    local v = redis.call('GET', 'lock:' .. key)
    if not v then
      redis.call('SET', 'lock:' .. key, id, 'ex', period)
      return 1
    elseif v == id then
      redis.call('EXPIRE', 'lock:' .. key, period)
      return 1
    end
    return 0
  ]],
  release = [[
    local key, id = KEYS[1], KEYS[2]
    local v = redis.call('GET', 'lock:' .. key)
    if not v or v == id then
      if v then
        redis.call('DEL', 'lock:' .. key)
      end
      return 1
    end
    return 0
  ]]
}

function lock_class:aquire()
  local lock = self

  local try_aquire_lock = function(red)
    local n = assert(red:eval(scripts.aquire, 3, lock.name, lock.id, lock.period, 0))
    return n == 1
  end

  if self.red:handle(try_aquire_lock, self.red.redis_rw) then
    update_time()
    lock.last_prolong_time = now()
    return true
  end

  return false
end

function lock_class:release()
  local lock = self

  local release_lock = function(red)
    local n = assert(red:eval(scripts.release, 2, lock.name, lock.id, 0))
    return n == 1
  end

  self.red:handle(release_lock, self.red.redis_rw)

  lock.last_prolong_time = 0

  return true
end

function lock_class:prolong()
  local lock = self

  local prolong_lock = function(red)
    local n = assert(red:eval(scripts.aquire, 3, lock.name, lock.id, lock.period, 0))
    return n == 1
  end

  update_time()

  if now() - lock.last_prolong_time < lock.period / 2 then
    return true
  end

  if self.red:handle(prolong_lock, self.red.redis_rw) then
    update_time()
    lock.last_prolong_time = time()
    return true
  end

  lock.last_prolong_time = 0

  return false
end

-- redis pipeline helpers --------------------

local function get_row_result(row)
  if type(row) == "table" then
    assert(row[1] ~= false, row[2])
  end
  return row
end

function _M.check_pipeline(result)
  return xpcall(foreachi, function(err)
    ngx_log(ERR, traceback())
    return err
  end, result, function(row)
    get_row_result(row)
  end)
end

----------------------------------------------

_M.get_row_result = get_row_result

return _M