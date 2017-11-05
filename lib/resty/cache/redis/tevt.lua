local redis = require "resty.cache.redis.wrapper"

local CONFIG = ngx.shared.config
local scope = CONFIG:get("ngx.caches.scope") or "ngx"

return redis.new {
  timeout   = CONFIG:get(scope .. ".caches.tevt.socket.timeout") or 30000,
  redis_ro  = CONFIG:get(scope .. ".caches.tevt.socket")         or "unix:logs/tevt.sock",
  redis_rw  = CONFIG:get(scope .. ".caches.tevt.socket")         or "unix:logs/tevt.sock",
  pool_size = 100,
  idle      = 10
}