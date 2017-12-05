local _M = {
  _VERSION = "0.2"
}

local ftype = {
  NUM    = 1,
  INT64  = 2,
  UINT64 = 3,
  STR    = 4,
  BOOL   = 5,
  OBJECT = 6,
  SET    = 7,
  LIST   = 8
}

local ipairs, pairs = ipairs, pairs
local xpcall = xpcall
local type = type
local traceback = debug.traceback
local tostring = tostring

local ngx_log = ngx.log
local ERR = ngx.ERR

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
  new_tab = function (narr, nrec) return {} end
end

local tconcat = table.concat
function _M.concat(t, sep)
  local out = new_tab(#t, 0)

  for j, v in ipairs(t)
  do
    out[j] = type(v) == "string" and v or tostring(v)
  end

  return tconcat(out, sep)
end

function _M.safe_call(f, ...)
  local ok, res, err = xpcall(f, function(err)
    ngx_log(ERR, err, "\n", traceback())
    return err
  end, ...)

  if not ok then
    return nil, res
  end

  return res, err
end

function _M.foreach_v(t, f)
  for _,v in pairs(t) do f(v) end
end

function _M.foreach(t, f)
  for k,v in pairs(t) do f(k, v) end
end

function _M.foreachi(t, f)
  for _,v in ipairs(t) do f(v) end
end

function _M.find_if(t, f)
  for k,v in pairs(t) do
    if f(k,v) then
      return k, v
    end
  end
end

function _M.find_if_i(t, f)
  for i,v in ipairs(t) do
    if f(v) then
      return { v, i }
    end
  end
end

function _M.totable(arg, field)
  return type(arg) == "table" and arg or {
    [field] = arg
  }
end

function _M.export_to(M)
  M.safe_call = _M.safe_call
  M.foreach_v = _M.foreach_v
  M.foreach   = _M.foreach
  M.foreachi  = _M.foreachi
  M.find_if   = _M.find_if
  M.find_if_i = _M.find_if_i
  M.totable   = _M.totable
  M.ftype     = ftype
end

do
  _M.ftype = ftype
end

return _M