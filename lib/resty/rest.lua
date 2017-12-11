local _M = {
  _VERSION = "0.2",

  content_type = {
    x_www_form_urlencoded = "application/x-www-form-urlencoded",
    json = "application/json"
  }
}

local cjson = require "cjson"
local http  = require "resty.http"

local CONFIG = ngx.shared.config

local scope = CONFIG:get("ngx.caches.scope") or "ngx"

local connect_timeout = CONFIG:get(scope .. ".caches.upsync.timeout.connect")                or 5000
local send_timeout    = CONFIG:get(scope .. ".caches.upsync.timeout.send")                   or 5000
local read_timeout    = CONFIG:get(scope .. ".caches.upsync.timeout.read")                   or 600000
local pool_size       = CONFIG:get(scope .. ".caches.upsync.socket.keepalive.pool_size")     or 10
local pool_inactive   = CONFIG:get(scope .. ".caches.upsync.socket.keepalive.pool_inactive") or 60

local encode_args = ngx.encode_args
local type = type

local ngx_var = ngx.var

local json_encode = cjson.encode
local json_decode = cjson.decode

local HTTP_OK = ngx.HTTP_OK
local HTTP_BAD_REQUEST = ngx.HTTP_BAD_REQUEST

local methods = {
  ["GET"]       = ngx.HTTP_GET,
  ["HEAD"]      = ngx.HTTP_HEAD,
  ["PUT"]       = ngx.HTTP_PUT,
  ["POST"]      = ngx.HTTP_POST,
  ["DELETE"]    = ngx.HTTP_DELETE,
  ["OPTIONS"]   = ngx.HTTP_OPTIONS,
  ["MKCOL"]     = ngx.HTTP_MKCOL,
  ["COPY"]      = ngx.HTTP_COPY,
  ["MOVE"]      = ngx.HTTP_MOVE,
  ["PROPFIND"]  = ngx.HTTP_PROPFIND,
  ["PROPPATCH"] = ngx.HTTP_PROPPATCH,
  ["LOCK"]      = ngx.HTTP_LOCK,
  ["UNLOCK"]    = ngx.HTTP_UNLOCK,
  ["PATCH"]     = ngx.HTTP_PATCH,
  ["TRACE"]     = ngx.HTTP_TRACE
}

local http_code_desc = {
  [ngx.HTTP_CONTINUE] = "CONTINUE",
  [ngx.HTTP_SWITCHING_PROTOCOLS] = "SWITCHING_PROTOCOLS",
  [ngx.HTTP_OK] = "OK",
  [ngx.HTTP_CREATED] = "CREATED",
  [ngx.HTTP_ACCEPTED] = "ACCEPTED",
  [ngx.HTTP_NO_CONTENT] = "NO_CONTENT",
  [ngx.HTTP_PARTIAL_CONTENT] = "PARTIAL_CONTENT",
  [ngx.HTTP_SPECIAL_RESPONSE] = "SPECIAL_RESPONSE",
  [ngx.HTTP_MOVED_PERMANENTLY] = "MOVED_PERMANENTLY",
  [ngx.HTTP_MOVED_TEMPORARILY] = "MOVED_TEMPORARILY",
  [ngx.HTTP_SEE_OTHER] = "SEE_OTHER",
  [ngx.HTTP_NOT_MODIFIED] = "NOT_MODIFIED",
  [ngx.HTTP_TEMPORARY_REDIRECT] = "TEMPORARY_REDIRECT",
  [ngx.HTTP_BAD_REQUEST] = "BAD_REQUEST",
  [ngx.HTTP_UNAUTHORIZED] = "UNAUTHORIZED",
  [ngx.HTTP_PAYMENT_REQUIRED] = "PAYMENT_REQUIRED",
  [ngx.HTTP_FORBIDDEN] = "FORBIDDEN",
  [ngx.HTTP_NOT_FOUND] = "NOT_FOUND",
  [ngx.HTTP_NOT_ALLOWED] = "NOT_ALLOWED",
  [ngx.HTTP_NOT_ACCEPTABLE] = "NOT_ACCEPTABLE",
  [ngx.HTTP_REQUEST_TIMEOUT] = "REQUEST_TIMEOUT",
  [ngx.HTTP_CONFLICT] = "CONFLICT",
  [ngx.HTTP_GONE] = "GONE",
  [ngx.HTTP_UPGRADE_REQUIRED] = "UPGRADE_REQUIRED",
  [ngx.HTTP_TOO_MANY_REQUESTS] = "TOO_MANY_REQUESTS",
  [ngx.HTTP_CLOSE] = "CLOSE",
  [ngx.HTTP_ILLEGAL] = "ILLEGAL",
  [ngx.HTTP_INTERNAL_SERVER_ERROR] = "INTERNAL_SERVER_ERROR",
  [ngx.HTTP_METHOD_NOT_IMPLEMENTED] = "METHOD_NOT_IMPLEMENTED",
  [ngx.HTTP_BAD_GATEWAY] = "BAD_GATEWAY",
  [ngx.HTTP_SERVICE_UNAVAILABLE] = "SERVICE_UNAVAILABLE",
  [ngx.HTTP_GATEWAY_TIMEOUT] = "GATEWAY_TIMEOUT",
  [ngx.HTTP_VERSION_NOT_SUPPORTED] = "VERSION_NOT_SUPPORTED",
  [ngx.HTTP_INSUFFICIENT_STORAGE] = "INSUFFICIENT_STORAGE"
}

function _M.get_http_method(m)
  return methods[m]
end

function _M.get_http_status_desc(code)
  return http_code_desc[code] or "UNKNOWN"
end

local function request(self, opts)
  local httpc = http.new()

  httpc:set_timeouts(self.connect_timeout or connect_timeout,
                     self.send_timeout or send_timeout,
                     self.read_timeout or read_timeout)

  local ok, err = httpc:connect(self.sock)
  if not ok then
    return false, nil, err
  end

  local headers = opts.headers or {}

  pcall(function()
    headers["pstxid"] = ngx_var.pstxid
  end)
  headers["Accept"] = "application/json"
  headers["Host"] = self.host or "default"

  if self.authorization and not headers["Authorization"] then
    headers["Authorization"] = self.authorization
  end

  local body = opts.body

  if body then
    if type(body) == "table" then
      local ct = headers["Content-Type"] or "nil"
      if ct == "application/x-www-form-urlencoded" then
        opts.body = encode_args(body)
      elseif ct == "application/json" then
        local ok, data = pcall(json_encode, body)
        if not ok then
          return false, nil, "failed to encode request body"
        end
        opts.body = data
      else
        error("body is a table and Content-Type header for object is: ", ct)
      end
    end
    headers["Content-Length"] = #opts.body
  end

  opts.headers = headers

  local resp, err = httpc:request(opts)

  -- restore body
  opts.body = body

  if not resp then
    return false, nil, err
  end

  resp.body, err = resp:read_body()

  if opts.close_connection or not httpc:set_keepalive(1000 * (self.pool_inactive or pool_inactive), self.pool_size or pool_size) then
    httpc:close()
  end

  err = resp.status ~= HTTP_OK and _M.get_http_status_desc(resp.status) or nil

  if resp.headers["Content-Type"] == "application/json" then
    if opts.no_decode then
      resp.data = resp.body
    else
      ok, resp.data = pcall(json_decode, resp.body)
      if not ok then
        resp.data = nil
        err = err or "INVALID_JSON_BODY"
      end
    end
  end

  return resp.status < HTTP_BAD_REQUEST, resp, err
end

--- @type Rest
local rest_class = {}

function _M.new(opts)
  opts.host = opts.host or opts.sock:match("^unix:logs/(.+)%.sock$")
  return setmetatable(opts, { __index = rest_class })
end

function rest_class:get(opts)
  opts.method = "GET"
  return request(self, opts)
end

function rest_class:post(opts)
  opts.method = "POST"
  return request(self, opts)
end

function rest_class:put(opts)
  opts.method = "PUT"
  return request(self, opts)
end

function rest_class:delete(opts)
  opts.method = "DELETE"
  return request(self, opts)
end

return _M