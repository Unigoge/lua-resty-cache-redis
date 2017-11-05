local _M = {
  _VERSION = "0.1"
}

local universal_cache = require "resty.cache.redis"

local shdict = require "shdict"
local CONFIG = shdict.new("config")

local ttl = CONFIG:get("ngx.caches.examle.ttl")

local Lx = CONFIG:object_get("ngx.caches.example.memory") or {}
Lx.L1, Lx.L2 = Lx.L1 or { ttl = 0, count = 0 }, Lx.L2 or { ttl = 10 }

local totable = universal_cache.totable

local cache = assert(universal_cache.new({
  cache_id = "EX",
  cache_name = "example",
  fields = {
    { name = "id",       ftype = universal_cache.ftype.NUM, pk = true },
    { name = "login",    ftype = universal_cache.ftype.STR, mandatory = true },
    { name = "password", ftype = universal_cache.ftype.NUM, mandatory = true },
    { name = "email",    ftype = universal_cache.ftype.STR, mandatory = true },
    { name = "locked",   ftype = universal_cache.ftype.BOOL }
  },
  indexes = {          -- redis indexes with zset
    { "login" }
  },
  memory = {
    name = "example",
    prefetch = false,  -- on-demand caching
    ttl = Lx.L2.ttl,
    L1 = Lx.L1
  }
}, {
  timeout   = CONFIG:get("ngx.caches.example.socket.timeout")   or 5000,
  pool_size = CONFIG:get("ngx.caches.example.socket.pool_size") or 100,
  idle      = CONFIG:get("ngx.caches.example.socket.pool_idle") or 60,
  redis_ro  = CONFIG:get("ngx.caches.example.ro.socket")        or "unix:logs/ttl.sock",
  redis_rw  = CONFIG:get("ngx.caches.example.rw.socket")        or "unix:logs/ttl.sock"
}))

function _M.get(id)
  return cache:get_memory_slave(totable(id, "id"))
end

function _M.search(data)
  return cache:search(data)
end

function _M.add(user)
  return cache:set(user)
end

function _M.replace(user)
  return cache:set(user, {
    overwrite = true
  })
end

function _M.update(user)
  return cache:set(user, {
    update = true
  })
end

function _M.delete(id)
  return cache:delete(totable(id, "id"))
end

function _M.dump()
  local result = {}
  cache:memory_scan(function(pk, data)
    tinsert(result, data)
  end)
  return result
end

function _M.purge()
  return cache:purge()
end

function _M.desc()
  return cache:desc()
end

function _M.init()
  return cache:init()
end

return _M