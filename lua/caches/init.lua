local _M = {
  _VERSION = "7.8.0"
}

local cjson = require "cjson"

local assert, unpack, tinsert, tconcat = assert, unpack, table.insert, table.concat
local type, pairs, ipairs, string, math = type, pairs, ipairs, string, math
local ngx_null, ngx_header, ngx_print, ngx_exit = ngx.null, ngx.header, ngx.print, ngx.exit
local ngx_get_method, ngx_get_uri_args = ngx.req.get_method, ngx.req.get_uri_args

local json_encode = cjson.encode

local HTTP_OK                     = ngx.HTTP_OK
local HTTP_NOT_FOUND              = ngx.HTTP_NOT_FOUND
local HTTP_BAD_REQUEST            = ngx.HTTP_BAD_REQUEST
local HTTP_INTERNAL_SERVER_ERROR  = ngx.HTTP_INTERNAL_SERVER_ERROR
local HTTP_CONFLICT               = ngx.HTTP_CONFLICT
local HTTP_METHOD_NOT_IMPLEMENTED = ngx.HTTP_METHOD_NOT_IMPLEMENTED
local HTTP_SPECIAL_RESPONSE       = ngx.HTTP_SPECIAL_RESPONSE

local function reply(opts)
  ngx.status = opts.status
  for k,v in pairs(opts.headers or {})
  do
    ngx_header[k] = v
  end
  for k,v in pairs(opts.cookies or {})
  do
    ngx.context.cookies:set {
      key = k,
      value = v,
      path = sep_in_cfg.cookie_opts.path,
      domain = sep_in_cfg.cookie_opts.domain
    }
  end
  local body = opts.body or {
    error = opts.status >= HTTP_SPECIAL_RESPONSE and get_http_status_desc(opts.status) or nil
  }
  if opts.body then
    local body = opts.body
    if type(body) == "table" then
      body = json_encode(opts.body)
      ngx_header["Content-Type"] = application_json
    end
    ngx_header["Content-Length"] = #body
    ngx_print(body)
  end
  return ngx_exit(opts.status)
end

local function success(val, code)
  ngx.status = code or HTTP_OK
  ngx_header["Content-Type"] = application_json
  local body = json_encode(type(val) == "table" and val or {
    result = val or "success"
  })
  ngx_header["Content-Length"] = #body
  ngx_print(body)
  return ngx_exit(code or HTTP_OK)
end

local function throw_internal_err(result, err, code)
  if result and result ~= ngx_null then
    return result
  end

  local status

  if result == ngx_null then
    if err == ngx_null then
      status = HTTP_CONFLICT
      err = "CONFLICT"
    else
      status = HTTP_NOT_FOUND
      err = "NOT_FOUND"
    end
  elseif err:match("bad args") then
    status = HTTP_BAD_REQUEST
  else
    status = code or HTTP_INTERNAL_SERVER_ERROR
  end

  reply {
    status = status,
    body = { error = err or "unexpected error"}
  }
end

local function success_or_throw(val, ...)
  throw_internal_err(val, ...)
  success(val, ...)
end

local function check_request_method(required)
  local method = ngx_get_method()
  if method ~= required then
    return reply {
      status = HTTP_BAD_REQUEST,
      body = { error = required .. " required" }
    }
  end
end

local function check_required_args(required)
  local ret = {}
  local uri_args = ngx_get_uri_args()
  for _,arg in ipairs(required)
  do
    for name, val in pairs(uri_args)
    do
      if name:lower() == arg then
        tinsert(ret, val)
      end
    end
  end
  if #ret ~= #required then
    throw_internal_err(nil, "bad args: " .. tconcat(required, ",") .. " required")
  end
  return unpack(ret)
end

local function thorow_on_unimplemented(mod, method)
  throw_internal_err(mod and method and mod[method], "unimplemented", HTTP_METHOD_NOT_IMPLEMENTED)
end

-- init PHASE

function _M.process()
  ngx.caches = ngx.caches or {}

  local caches = ngx.caches

  -- common methods

  caches.reply                   = reply
  caches.request_failed          = reply
  caches.success                 = success
  caches.throw_internal_err      = throw_internal_err
  caches.success_or_throw        = success_or_throw
  caches.check_request_method    = check_request_method
  caches.check_required_args     = check_required_args
  caches.thorow_on_unimplemented = thorow_on_unimplemented
end

return _M