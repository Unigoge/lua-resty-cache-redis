# lua-resty-cache-redis
Redis caching layer for Redis and shared memory with HTTP upsync and declarative description

## Status
Under development

## Requirenments
This solution requires modified lua-nginx-module (added shdic:fun methods).  
All of the requirenments are downloaded automatically in build.sh.  

  * [lua-nginx-module](https://github.com/ZigzagAK/lua-nginx-module/tree/mixed)
  * [lua-resty-redis](https://github.com/openresty/lua-resty-redis)
  * [lua-resty-lock](https://github.com/openresty/lua-resty-lock)
  * [lua-resty-http](https://github.com/pintsized/lua-resty-http)
  * [lua-resty-lrucache](https://github.com/openresty/lua-resty-lrucache)
  * [nginx-resty-auto-healthcheck-config](https://github.com/ZigzagAK/nginx-resty-auto-healthcheck-config)
  * [cjson](https://github.com/ZigzagAK/lua-cjson)

## Build
./build.sh

## Configuration
```
init_by_lua_block {
  local shdict = require "shdict"
  local CONFIG = shdict.new("config")

  CONFIG:set("ngx.caches.scope", "ngx")

  -- system caches

  CONFIG:set("ngx.caches.tevt.socket.timeout", 5000)
  CONFIG:set("ngx.caches.tevt.socket", "unix:logs/tevt.sock")

  CONFIG:set("ngx.caches.system.socket.timeout", 5000)
  CONFIG:set("ngx.caches.system.socket", "unix:logs/system.sock")

  -- example caches

  CONFIG:set("ngx.caches.example.socket.timeout", 5000)
  CONFIG:set("ngx.caches.example.socket.pool_size", 100)
  CONFIG:set("ngx.caches.example.socket.pool_idle", 10)
  CONFIG:set("ngx.caches.example.ro.socket", "unix:logs/redis.sock")
  CONFIG:set("ngx.caches.example.rw.socket", "unix:logs/redis.sock")
  CONFIG:set("ngx.caches.example.debug", true)
  CONFIG:object_set("ngx.caches.example.memory", {
    L1 = { ttl = 10, count = 1000 }, -- worker local
    L2 = {}                          -- shared memory no ttl
  })

  CONFIG:set("ngx.caches.example_upsync.socket.timeout", 5000)
  CONFIG:set("ngx.caches.example_upsync.socket.pool_size", 100)
  CONFIG:set("ngx.caches.example_upsync.socket.pool_idle", 10)
  CONFIG:set("ngx.caches.example_upsync.ro.socket", "unix:logs/redis.sock")
  CONFIG:set("ngx.caches.example_upsync.rw.socket", "unix:logs/redis.sock")
  CONFIG:set("ngx.caches.example_upsync.sync_interval", 10)
  CONFIG:set("ngx.caches.example_upsync.debug", true)

  -- common dict memory L1/L2 cache settings
  CONFIG:object_set("ngx.caches.dict.memory", {
    L1 = { ttl = 10, count = 1000 }, -- worker local
    L2 = {}                          -- shared memory no ttl
  })

  -- upsync http

  CONFIG:set("ngx.upsync.timeout.connect", 5000)
  CONFIG:set("ngx.upsync.timeout.send", 5000)
  CONFIG:set("ngx.upsync.timeout.read", 5000)
  CONFIG:set("ngx.upsync.socket.keepalive.pool_size", 100)
  CONFIG:set("ngx.upsync.socket.keepalive.pool_inactive", 60000)

  -- init api
  local init = require "caches.init"
  init.process()
}

# memory dictionaries (L1 cache)

lua_shared_dict example        100m;
lua_shared_dict example_upsync 100m;
```

## Without upsync and with caching in-memory LRU declaration
```
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
```

## With upsync and with caching in-memory declaration
```
local _M = {
  _VERSION = "0.1"
}

local universal_cache = require "resty.cache.redis_with_upsync"

local cjson = require "cjson"

local shdict = require "shdict"
local CONFIG = shdict.new("config")

local Lx = CONFIG:object_get("ngx.caches.dict.memory") or {}
Lx.L1, Lx.L2 = Lx.L1 or { ttl = 10, count = 1000 }, Lx.L2 or {}

local tinsert = table.insert
local totable = universal_cache.totable
local tostring = tostring

local function upsync_content_handler(self, resp)
  local exists, err = self:keys()
  if not exists then
    self:warn("upsync_content_handler()", function()
      return err
    end)
    return
  end

  universal_cache.foreachi(resp, function(attr)
    self:debug("upsync_content_handler()", function()
      return cjson.encode(attr)
    end)

    local ok, err = self:set({
      id = attr.id,
      name = attr.name,
      public = attr.public
    }, { 
      overwrite = true
    })
    if not ok then
      self:warn("upsync_content_handler()", function()
        return "failed to add attr: ", cjson.encode(attr), " : ", err
      end)
    end

    exists[tostring(attr.id)] = nil

    self:prolong_lock()
  end)

  universal_cache.foreach(exists, function(id)
    self:debug("upsync_content_handler()", function()
      return "delete obsolete id=", id
    end)
    local ok, err = self:delete { id = id }
    if not ok then
      self:err("upsync_content_handler()", function()
        return err
      end)
    end
  end)
end

local cache = assert(universal_cache.new({
  cache_id = "EU",
  cache_name = "example_upsync",
  fields = {
    { name = "id",     ftype = universal_cache.ftype.NUM, pk = true },
    { name = "name",   ftype = universal_cache.ftype.STR, mandatory = true },
    { name = "public", ftype = universal_cache.ftype.STR, mandatory = true },
  },
  memory = {
    name = "example_upsync",
    prefetch = true,
    ttl = Lx.L2.ttl,
    L1 = Lx.L1,
    indexes = {
      { "name" },
      { "public" }
    }
  },
  upsync = {
    socket = "unix:logs/stub.sock",
    uri = "/attr",
    content_handler = upsync_content_handler
  }
}, {
  storage = "example_upsync"
}))

function _M.get(id)
  return cache:get_memory_slave(totable(id, "id"))
end

function _M.search(data)
  local result = {}  
  cache:get_by_index(data, {
    callback = function(key, data)
      tinsert(result, data)
    end
  })
  return result
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
  return cache:init_upsync()
end

return _M
```
