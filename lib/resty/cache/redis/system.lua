--- @module System
--  @return #System

local redis = require "resty.cache.redis.wrapper"

local CONFIG = ngx.shared.config
local scope = CONFIG:get("ngx.caches.scope") or "ngx"

local system = redis.new {
  timeout   = CONFIG:get(scope .. ".caches.system.socket.timeout") or 5000,
  redis_ro  = CONFIG:get(scope .. ".caches.system.socket")         or "unix:logs/system.sock",
  redis_rw  = CONFIG:get(scope .. ".caches.system.socket")         or "unix:logs/system.sock",
  pool_size = 100,
  idle      = 10
}

--- @type System
local system_class = {}

function system_class:socket()
  return system:rw_socket()
end

function system_class:handle(f, ...)
  return system:handle(f, self:socket(), ...)
end

function system_class:create_lock(name, period)
  return system:create_lock(name, period)
end

return setmetatable({}, { __index = system_class })