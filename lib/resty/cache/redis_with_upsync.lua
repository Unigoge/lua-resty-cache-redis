--- @module RedisWithUpsync

local _M = {
  _VERSION = "0.2"
}

local job = require "job"
local rest = require "resty.rest"
local redis = require "resty.cache.redis"

local update_time = ngx.update_time
local ngx_now = ngx.now
local xpcall = xpcall
local pairs = pairs
local ngx_null = ngx.null
local traceback = debug.traceback

local HTTP_NOT_MODIFIED = ngx.HTTP_NOT_MODIFIED
local HTTP_OK = ngx.HTTP_OK

local function now()
  update_time()
  return ngx_now()
end

local assert = assert
local setmetatable, getmetatable = setmetatable, getmetatable

local function merge_index(dst, src)
  local mt__index = getmetatable(src).__index
  for k,v in pairs(dst)
  do
    mt__index[k] = v
  end
  return mt__index
end

--- @type RedisWithUpsync
--  @extends resty.cache.redis#Redis
--  @field resty.cache.redis.wrapper#RedisDistributedLock lock
local redis_with_sync = {}

function redis_with_sync:parse(resp)
  if #resp.data == 0 then
    self:warn("upsync_content_handler()", function()
      return "response is empty"
    end)
    return
  end
  self.upsync.content_handler(self, resp.data)
end

--- @param #RedisWithUpsync self
function redis_with_sync:prolong_lock()
  return self.lock:prolong()
end

--- @param #RedisWithUpsync self
function redis_with_sync:aquire_lock()
  return self.lock:aquire()
end

--- @param #RedisWithUpsync self
function redis_with_sync:do_upsync()
  local start = now()

  local lm = self:last_modified()
  if lm ~= ngx_null then
    self.upsync.headers["If-Modified-Since"] = lm
  end

  self:info("upsync()", function()
    return "begin (last modified: ", lm, ")"
  end)

  local ok, resp, err = self.upsync_server[self.upsync.method](self.upsync_server, {
    path = self.upsync.uri,
    body = self.upsync.body,
    headers = self.upsync.headers,
    no_decode = self.upsync.no_decode,
    close_connection = true
  } )

  self:info("upsync()", function()
    return "fetch=", now() - start, " seconds"
  end)

  if not ok then
    self:err("upsync()", function()
      return err or "upstream is not available"
    end)
    return
  end

  if resp.status == HTTP_NOT_MODIFIED then
    self:info("upsync()", function()
      return "NOT_MODIFIED"
    end)
    return
  end

  if resp.status ~= HTTP_OK then
    self:warn("upsync()", function()
      return err or resp.data or "unknown error"
    end)
    return
  end

  start = now()

  if xpcall(redis_with_sync.parse, function(err)
    self:err("upsync()", err, "\n", traceback())
    return err
  end, self, resp) then
    lm = resp.headers["Last-Modified"]
    if lm then
      self:update_last_modified(lm)
    end
  end

  self:info("upsync()", function()
    return "parse=", now() - start, " seconds"
  end)
end

--- @param #RedisWithUpsync self
function redis_with_sync:init_upsync()
  local prefetch_job = self:init()

  if not self.upsync.interval then
    return prefetch_job
  end

  self.upsync_server = rest.new {
    sock = self.upsync.socket
  }

  self.upsync.method = self.upsync.method or "get"
  self.upsync.headers = self.upsync.headers or {}

  assert(self.upsync_server[self.upsync.method], self.upsync.method .. " is not found (get, post)")

  self.lock = self:create_lock("SEP_CACHE_UPSYNC:" .. self.cache_name, self.upsync.interval or 60)

  local sync_job = job.new("upsync " .. self.cache_name, function()
    self:aquire_lock()
    self:do_upsync()
    return true
  end, self.upsync.interval > 0 and self.upsync.interval or 60)

  if prefetch_job then
    sync_job:wait_for(prefetch_job)
  end

  sync_job:run()

  return prefetch_job
end

--- @return #RedisWithUpsync
--  @param #table opts
--  @param #table redis_opts
function _M.new(opts, redis_opts)
  assert(opts.upsync, "upsync required { socket, uri, content_handler }")
  opts.prefetch = true
  local CONFIG = ngx.shared.config
  local scope = CONFIG:get("ngx.caches.scope") or "ngx"
  local storage = redis_opts and redis_opts.storage or "dictionary"
  opts.upsync.interval = opts.upsync.interval or CONFIG:get(scope .. ".caches." .. opts.cache_name .. ".sync_interval")
  if redis_opts and not redis_opts.redis_rw then
    redis_opts = nil
  end
  local cache = redis.new(opts, redis_opts or {
    timeout   = CONFIG:get(scope .. ".caches." .. storage .. ".socket.timeout")   or 5000,
    pool_size = CONFIG:get(scope .. ".caches." .. storage .. ".socket.pool_size") or 100,
    idle      = CONFIG:get(scope .. ".caches." .. storage .. ".socket.pool_idle") or 60,
    redis_ro  = CONFIG:get(scope .. ".caches." .. storage .. ".ro.socket")        or "unix:logs/" .. storage .. "-ro.sock",
    redis_rw  = CONFIG:get(scope .. ".caches." .. storage .. ".rw.socket")        or "unix:logs/" .. storage .. "-rw.sock"
  })
  return assert(setmetatable(cache, {
    __index = merge_index(redis_with_sync, cache)
  }))
end

do
  local common = require "resty.cache.common"
  common.export_to(_M)
end

return _M