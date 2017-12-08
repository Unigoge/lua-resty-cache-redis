--- @module RedisLogServer

local _M = {
  _VERSION = "0.1"
}

local cjson = require "cjson"
local common = require "resty.cache.common"

local assert, type, tonumber = assert, type, tonumber
local pairs, ipairs = pairs, ipairs
local tinsert, tconcat = table.insert, common.concat
local min = math.min
local ngx_null = ngx.null
local json_encode = cjson.encode

local function make_log_key(T, logid)
  return "L:" .. T .. ":" .. logid
end

--- @type RedisLogger
--  @field resty.cache.redis.system#System system
--  @field resty.cache.redis.tevt#Tevt tevt
local logger = {}

--- @return #RedisLogger
function _M.new(cache)
  local CONFIG = ngx.shared.config
  local scope = CONFIG:get("ngx.caches.scope") or "ngx"
  local l = {
    ttl = CONFIG:get(scope .. ".caches.log.ttl") or 3600,
    cache = cache,
    cache_id = cache.cache_id or cache.cache_name,
    tevt = require "resty.cache.redis.tevt",
    system = require "resty.cache.redis.system"
  }
  return setmetatable(l, { __index = logger})
end

function logger:debug(f, fun)
  self.cache:debug(f, fun)
end

function logger:warn(f, fun)
  self.cache:warn(f, fun)
end

--- @param #RedisLogger self
function logger:log_id()
  local log_id = function(red)
    local logid = assert(red:get("L:" .. self.cache_id .. ":ID"))
    return logid ~= ngx_null and tonumber(logid) or 0
  end
  return self.system:handle(log_id, self.system:socket())
end

--- @param #RedisLogger self
function logger:next_log_id(i)
  local next_log_id = function(red)
    return assert(red:incrby("L:" .. self.cache_id .. ":ID", i or 1))
  end
  return self.system:handle(next_log_id, self.system:socket())
end

--- @param #RedisLogger self
function logger:log_event(ev)
  return self:log_events { ev }
end

--- @param #RedisLogger self
function logger:log_events(ev)
  local log_events = function(red)
    local logid = self:next_log_id(#ev)

    red:init_pipeline()

    for i,event in ipairs(ev)
    do
      local key = make_log_key(self.cache_id, logid - #ev + i)

      self:debug("log_event()", function()
        return "logid=", logid, " ", json_encode(event)
      end)

      red:hmset(key, { ev = event.ev,
                       pk = cjson.encode(event.pk or {}),
                       data = event.data and cjson.encode(event.data) or nil,
                       ttl = event.ttl } )

      red:expire(key, self.ttl)
    end

    assert(red:commit_pipeline())

    return true
  end

  return self.tevt:handle(log_events, self.tevt:rw_socket())
end

--- @param #RedisLogger self
function logger:get_events(id, limit)
  assert(id, "bad args: id required")

  id = tonumber(id)

  local log_get = function(red)
    local response = { events = {} }
    local ok, err
    local log_event
    local logid = self:log_id()

    if logid <= id then
      response.logid = logid
      return response
    end

    if limit then
      logid = min(logid, id+limit)
    end

    for j=id+1,logid,100
    do
      red:init_pipeline()

      for i=j,min(logid, j+99)
      do
        red:hgetall(make_log_key(self.cache_id, i))
      end

      local results = assert(red:commit_pipeline())

      for i=1,#results
      do
        log_event = results[i]
        if log_event == ngx_null or #log_event == 0 then
          -- no key ???
          goto continue
        end

        if log_event[1] == false then
          -- error hgetall (why?): { false, err }
          self:warn("get_events()", function()
            return "logid=", i, " ", log_event[2]
          end)
          goto continue
        end

        log_event = red:array_to_hash(log_event)
        log_event.logid = j + i - 1
        log_event.pk = cjson.decode(log_event.pk)
        log_event.data = log_event.data and cjson.decode(log_event.data) or nil

        tinsert(response.events, log_event)

:: continue ::
        response.logid = j + i -1
      end
    end

    return response
  end

  return self.tevt:handle(log_get, self.tevt:rw_socket())
end

return _M