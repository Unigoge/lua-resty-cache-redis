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