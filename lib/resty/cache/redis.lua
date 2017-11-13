local _M = {
  _VERSION = "0.1"
}

local cjson = require "cjson"
local job = require "job"
local common = require "resty.cache.common"
local redis = require "resty.cache.redis.wrapper"
local log_server = require "resty.cache.log_server"
local system = require "resty.cache.redis.system"

-- import types
local ftype = common.ftype

local tinsert, tconcat, tremove, tsort = table.insert, table.concat, table.remove, table.sort
local tostring, tonumber = tostring, tonumber
local pairs, ipairs, next = pairs, ipairs, next
local assert, type, unpack, pcall = assert, type, unpack, pcall
local setmetatable = setmetatable
local max, min, floor = math.max, math.min, math.floor
local crc32 = ngx.crc32_short
local update_time = ngx.update_time
local ngx_now, ngx_time = ngx.now, ngx.time
local ngx_var = ngx.var
local ngx_log = ngx.log
local ngx_null = ngx.null
local sleep = ngx.sleep
local worker_exiting = ngx.worker.exiting
local DEBUG, INFO, WARN, ERR = ngx.DEBUG, ngx.INFO, ngx.WARN, ngx.ERR
local thread_spawn, thread_wait = ngx.thread.spawn, ngx.thread.wait
local json_encode, json_decode = cjson.encode, cjson.decode
local safe_call = common.safe_call

local events = {
  SET    = "set",
  UPDATE = "update",
  DELETE = "delete",
  PURGE  = "purge"
}

local function fake_fun(...)
  return ...
end

-- ZLIB support for fields

local zlib_loaded, zlib = pcall(require, "ffi-zlib")
local inflate = zlib_loaded and function(input)
  local decompressed = {}
  local offset = 1
  local length = #input
  local ok, err = zlib.inflateGzip(function(sz)
    if offset > length then
      return
    end
    local chunk = input:sub(offset, min(offset + sz - 1, length))
    offset = offset + sz
    return chunk
  end, function(chunk)
    tinsert(decompressed, chunk)
  end, 16384)
  return tconcat(decompressed, "")
end or fake_fun

local deflate = zlib_loaded and function(input)
  local compressed = {}
  local offset = 1
  local length = #input
  local ok, err = zlib.deflateGzip(function(sz)
    if offset > length then
      return
    end
    local chunk = input:sub(offset, min(offset + sz - 1, length))
    offset = offset + sz
    return chunk
  end, function(chunk)
    tinsert(compressed, chunk)
  end, 16384, {
    strategy = zlib.Z_HUFFMAN_ONLY,
    level = zlib.Z_BEST_COMPRESSION
  })
  return tconcat(compressed, "")
end or fake_fun

-- time helpers

local function now()
  update_time()
  return ngx_now()
end

local function time()
  update_time()
  return ngx_time()
end

local foreachi, foreach, find_if, find_if_i =
  common.foreachi, common.foreach, common.find_if, common.find_if_i

-- helpers

local function check_number(n)
  if type(n) == "number" then
    return n
  end
  local out = tonumber(n)
  return tostring(out) == n and out or n
end

local function check_boolean(v)
  if type(v) == "boolean" then
    return v
  end
  if not v or type(v) == "number" then
    return v and v ~= 0
  end
  v = v:lower()
  return v == "true" or v == "y" or v == "yes" or v == "ok" or v == "1"
end

local function compare(old, new)
  if not pcall(foreach, new, function(k,nv)
    local ov = old[k]
    if type(nv) ~= "table" then
      assert((ov and ov == nv) or (not ov and nv == "nil"), "not matched")
      return
    end
    -- table value
    tsort(ov,function(l,r) return l < r end)
    tsort(nv,function(l,r) return l < r end)
    assert(compare(ov, nv), "not matched")
  end) then
    return false
  end
  return true
end

local function equals(old, new)
  return compare(old, new) and compare(new, old)
end

-- update cache description

local function cache_desc_fixup(cache)
  if cache.fields[#cache.fields].dbfield then
    return
  end

  local update = function(red)
    local cache_key = "cache:" .. cache.cache_name

    local old = assert(red:hgetall(cache_key))

    local desc = old == ngx_null and {
      fields = {}
    } or red:array_to_hash(old)

    desc.fields  = desc.fields  and json_decode(desc.fields)  or {}
    desc.indexes = desc.indexes and json_decode(desc.indexes) or {}

    local function dbfield(name)
      local field, i = unpack(find_if_i(desc.fields, function(field)
        return field.name == name
      end) or {
        { dbfield = tostring(assert(red:hincrby(cache_key, "f_index", 1))) }, -1
      })
      return field.dbfield
    end

    local indexes_changed

    local function dbindex(hash)
      local index, i = unpack(find_if_i(desc.indexes, function(idx)
        return idx.hash == hash
      end) or {
        { id = tostring(assert(red:hincrby(cache_key, "i_index", 1))) }, -1
      })
      if i == -1 then
        -- new index
        desc.i_index = index.id
      else
        tremove(desc.indexes, i)
      end
      return index.id, hash
    end

    cache.fields_indexed = {}

    foreachi(cache.fields, function(field)
      field.dbfield = dbfield(field.name)
      if field.indexed then
        tinsert(cache.fields_indexed, field.dbfield)
      end
    end)

    foreachi(cache.indexes, function(index)
      local hash = ngx.encode_base64(tconcat(index.fields, ":"), true)
      index.id, index.hash = dbindex(hash)
    end)

    foreachi(desc.indexes, function(index)
      index.obsolete = true
      tinsert(cache.indexes, index)
    end)

    cache.i_crc32 = tostring(crc32(json_encode(cache.indexes)))

    return assert(red:hmset(cache_key, { fields   = json_encode(cache.fields),
                                         indexes  = json_encode(cache.indexes),
                                         ttl      = cache.ttl,
                                         cache_id = cache.cache_id,
                                         i_crc32  = cache.i_crc32 }))
  end

  return system:handle(update)
end

local function check_pk(fields, pk)
  local pk_tab = (type(pk) == "table" and pk[1]) and pk or { pk }
  for _, pk in ipairs(pk_tab)
  do
    local tmp = {}
    foreach(pk, function(k,v)
      tmp[k:lower()] = v
      pk[k] = nil
    end)
    foreach(tmp, function(k,v)
      pk[k] = v
    end)
    for i=1,#fields
    do
      local field = fields[i]
      local v = pk[field.name]
      if field.pk and (not v or v == "" or type(v) == "table") then
        return nil, "bad args: " .. field.name .. " required and must be a scalar"
      end
    end
  end
  return true
end

local function check_simple_types_out(fields, data)
  foreachi(fields, function(field)
    local name = field.name
    local value = data[name]

    if value then
      if value == "null" or value == ngx_null then
        data[name] = ngx_null
      elseif value == "nil" then
        data[name] = nil
      elseif field.ftype == ftype.NUM then
        data[name] = check_number(value)
      elseif field.ftype == ftype.STR then
        data[name] = tostring(value)
      elseif field.ftype == ftype.BOOL then
        data[name] = check_boolean(value)
      end
    end
  end)
end

local function check_in(cache, data, update)
  local fields = cache.fields

  local tmp = {}
  foreach(data, function(k,v)
    tmp[k:lower()] = v
    data[k] = nil
  end)
  foreach(tmp, function(k,v)
    data[k] = v
  end)

  for i=1,#fields
  do
    local field = fields[i]
    local name = field.name
    local value = data[name]

    if value then
      if value == ngx_null then
        data[name] = ngx_null
      elseif value == "nil" then
        if field.mandatory then
          return nil, "bad args: mandatory field can't be nil"
        end
        if field.indexed then
          return nil, "bad args: indexed field can't be nil"
        end
      elseif field.ftype == ftype.NUM then
        data[name] = check_number(value)
      elseif field.ftype == ftype.STR then
        data[name] = tostring(value)
      elseif field.ftype == ftype.BOOL then
        data[name] = check_boolean(value)
      elseif field.ftype == ftype.OBJECT then
        -- skip
      elseif field.ftype == ftype.SET then
        -- skip
      elseif field.ftype == ftype.LIST then
        -- skip
      end
    elseif field.partof and data[field.partof] then
      -- skip
    elseif field.skip and not field.partof then
      -- skip
    else
      if field.pk or (field.mandatory and not update) then
        return nil, "bad args: " .. field.name .. " required"
      end
    end
  end

  return true
end

local function defaults(fields, data)
  foreachi(fields, function(field)
    local name = field.name
    local value, default = data[name], field.default
    if not value and default then
      data[name] = default
    end
  end)
end

local function to_db(cache, data)
  local set, del = { ["#i"] = cache.i_crc32 }, {}
  local fields = cache.fields

  for i=1,#fields
  do
    local field = fields[i]
    local name, dbfield = field.name, field.dbfield
    if not field.skip then
      local value = data[name]
      if value == "nil" then
        -- special value for remove field
        tinsert(del, { dbfield, field.ftype == ftype.SET or field.ftype == ftype.LIST } )
      elseif field.ftype == ftype.SET or field.ftype == ftype.LIST then
        local object
        if not value then
          -- composing from source fields
          object = {}
          foreachi(type(field.source) == "table" and field.source or { field.source }, function(source)
            object[#object + 1] = data[source] and (data[source] ~= ngx_null and data[source] or "null") or "nil"
          end)
          set[dbfield] = tconcat(object, "|")
        else
          -- get from value
          set[dbfield] = {}
          foreachi(type(value) == "table" and value or { value }, function(v)
            object = {}
            foreachi(type(field.source) == "table" and field.source or { field.source }, function(source)
              object[#object + 1] = v[source] and (v[source] ~= ngx_null and v[source] or "null") or "nil"
            end)
            tinsert(set[dbfield], tconcat(object, "|"))
          end)
        end
      elseif field.ftype == ftype.OBJECT then
        local x = json_encode(value)
        set[dbfield] = field.gzip and deflate(x) or x
      elseif field.ftype == ftype.STR then
        set[dbfield] = value == ngx_null and "null" or (field.gzip and deflate(value) or value)
      else
        set[dbfield] = value == ngx_null and "null" or value
      end
    end
  end

  return set, del
end

local function from_db(fields, data)
  local out = {}

  for i=1,#fields
  do
    local field = fields[i]
    local name, dbfield = field.name, field.dbfield
    local value = data[dbfield]
    if value then
      if field.ftype == ftype.SET or field.ftype == ftype.LIST then
        local source = type(field.source) == "table" and field.source or { field.source }
        if type(value) == "table" then
          out[name] = {}
          for j=1,#value
          do
            local object = {}
            local k = 1
            for f in value[j]:gmatch("([^|]+)")
            do
              if f ~= "nil" then
                object[source[k]] = f
              end
              k = k + 1
            end
            check_simple_types_out(fields, object)
            out[name][j] = type(field.source) == "table" and object or object[source[1]]
          end
        end
      elseif field.ftype == ftype.OBJECT then 
        out[name] = value == "null" and ngx_null or json_decode(
          field.gzip and inflate(value) or value
        )
      elseif field.ftype == ftype.STR then 
        out[name] = value == "null" and ngx_null or (field.gzip and inflate(value) or value)
      else
        out[name] = value == "null" and ngx_null or value
      end
    end
  end

  return out
end

local cache_class = {}

local function pstxid()
  local ok, id = pcall(function()
    return ngx_var.pstxid or ngx_var.request_id
  end)
  return ok and id or "job"
end

function cache_class:debug(f, fun)
  if self.debug_enabled then
    ngx_log(DEBUG, "[", pstxid() , "] ", self.cache_name, "_", f, " ", fun())
  end
end

function cache_class:info(f, fun)
  ngx_log(INFO, "[", pstxid() , "] ", self.cache_name, "_", f, " ", fun())
end

function cache_class:warn(f, fun)
  ngx_log(WARN, "[", pstxid() , "] ", self.cache_name, "_", f, " ", fun())
end

function cache_class:err(f, fun)
  ngx_log(ERR, "[", pstxid() , "] ", self.cache_name, "_", f, " ", fun())
end

function cache_class:make_pk(data)
  local is_multi = type(data) == "table" and data[1]
  local data_tab = is_multi and data or { data }
  local result = {}
  for _, data in ipairs(data_tab)
  do
    local key, pk = { self.cache_id }, {}
    foreachi(self.fields, function(field)
      if field.pk then
        local name = field.name
        local value = data[name] or "*"
        pk[name] = value
        tinsert(key, value ~= ngx_null and value or "null")
      end
    end)
    if not is_multi then
      return pk, tconcat(key, ":")
    end
    tinsert(result, { pk, tconcat(key, ":") })
  end
  return result
end

function cache_class:key2pk(key)
  local fields = self.fields
  local pk = key:match("^" .. self.cache_id) and key:match("^" .. self.cache_id .. ":(.+)$") or key
  local t, i = {}, 1
  for v in pk:gmatch("([^:]+)")
  do
    while i <= #fields and not fields[i].pk do i = i + 1 end
    assert(i <= #fields, "invalid key")
    t[fields[i].name] = v ~= "null" and v or ngx_null
    i = i + 1
  end
  return t
end

function cache_class:make_key(data)
  local pattern = "^" .. self.cache_id .. ":(.+)$"
  local tab, key = self:make_pk(data)
  if key then
    -- single key operation
    return key:match(pattern)
  end
  -- fix all keys
  for i,pair in ipairs(tab)
  do
    tab[i] = { key = pair[2]:match(pattern) }
  end
  return tab
end

function cache_class:keys(redis_xx)
  local keys = function(red)
    local keys = assert(self.redis:zscan(red, self.PK, "*"))
    local h = {}
    foreachi(keys, function(v) h[tostring(v.key)] = true end)
    return h, #keys
  end

  return self.redis:handle(keys, redis_xx or self.redis_rw)
end

function cache_class:get_unsafe(pk, redis_xx, getter)
  if not pk then
    return nil, "bad args: pk required"
  end

  local ok, err = check_pk(self.fields, pk)
  if not ok then
    return nil, err
  end

  pcall(cache_desc_fixup, self)

  local get = function(red)
    local response = {}
    local ok, err, data, expires

    local key = self:make_key(pk)
    -- if type(key) is a table we have a multikey operation 
    local keys = type(key) == "table" and key or { { key = key } }
    local PK = self.PK

    if type(key) ~= "table" then
      if key:match("[%?%*%[]") then
        keys = assert(self.redis:zscan(red, PK, key))
      end
    end

    getter = getter or function(data) tinsert(response, data) end
    local callback_on_get = self.callback_on_get or fake_fun

    local offset = 1

    while offset <= #keys
    do
      local j = offset

      red:init_pipeline()

      for i=1,100
      do
        local k = keys[offset].key
        local key = self.cache_id .. ":" .. k
        red:hgetall(key)
        red:zscore(PK, k)
        foreachi(self.sets, function(field)
          red:smembers(key .. ":" .. field.dbfield)
        end)
        foreachi(self.lists, function(field)
          red:lrange(key .. ":" .. field.dbfield, 0, -1)
        end)
        offset = offset + 1
        if offset > #keys then
          break
        end
      end

      local results = assert(red:commit_pipeline())

      for i=1,#results,2 + #self.sets + #self.lists
      do
        local key = keys[j].key
        j = j + 1
        data = redis.get_row_result(results[i])
        expires = redis.get_row_result(results[i + 1])
        expires = (expires ~= ngx_null and expires ~= "inf") and tonumber(expires) or nil
        if #data ~= 0 and (not expires or expires > now()) then
          local object = red:array_to_hash(data)
          for k = 1,#self.sets
          do
            local members = assert(results[i + 1 + k])
            object[self.sets[k].dbfield] = members
          end
          for k = 1,#self.lists
          do
            local members = assert(results[i + 1 + #self.sets + k])
            object[self.lists[k].dbfield] = members
          end
          data = from_db(self.fields, object)
          check_simple_types_out(self.fields, data)
          self:debug("get()", function()
            return "pk=", json_encode(self:key2pk(key)), " data=", json_encode(data) 
          end)
          foreach(self:key2pk(key), function(k,v) data[k] = v end)
          callback_on_get(red, data)
          getter(data, key, expires and expires - now() or nil)
        end
      end
    end

    return #response ~= 0 and response or ngx_null
  end

  return self.redis:handle(get, redis_xx or self.redis_rw)
end

function cache_class:get(pk, getter)
  return safe_call(self.get_unsafe, self, pk, self.redis_ro, getter)
end

function cache_class:get_master(pk, getter)
  return safe_call(self.get_unsafe, self, pk, self.redis_rw, getter)
end

-- search by indexes

local function index_keys(self, data)
  local index_keys = {}
  foreachi(self.indexes, function(index)
    local index_key = { index.id }
    foreachi(index.fields, function(name)
      local val = data[name]
      if val then
        tinsert(index_key, val)
      end
    end)
    if #index_key - 1 == #index.fields then
      -- found
      tinsert(index_keys, tconcat(index_key, ":"))
    end
  end)
  return #index_keys ~= 0 and index_keys or nil
end

function cache_class:search_unsafe(data, redis_xx, getter)
  if not data then
    return nil, "bad args: data required"
  end

  pcall(cache_desc_fixup, self)

  local ikeys = index_keys(self, data)
  if not ikeys then
    return nil, "bad args: not enough data for indexes"
  end

  redis_xx = redis_xx or self.redis_rw

  local search_pk = function(red, ikey)
    return assert(red:smembers(ikey))
  end

  local pk = {}

  foreachi(ikeys, function(iikey)
    foreachi(self.redis:handle(search_pk, redis_xx, self.cache_id .. ":" .. iikey), function(ikey)
      tinsert(pk, self:key2pk(ikey))
    end)
  end)

  if #pk == 0 then
    return self.redis:handle(function(red)
      local logid, err = red:get("L:" .. self.cache_name .. ":ID")
      return logid and {} or nil, err
    end, redis_xx)
  end

  return self:get_unsafe(pk, redis_xx, getter)
end

function cache_class:search(data, getter)
  return safe_call(self.search_unsafe, self, data, self.redis_ro, getter)
end

function cache_class:search_master(data, getter)
  return safe_call(self.search_unsafe, self, data, self.redis_rw, getter)
end

local function with_indexes(self, pk, new, old, fun)
  foreachi(self.indexes, function(index)
    local index_key, old_index_key = { index.id }, { index.id }
    foreachi(index.fields, function(name)
      tinsert(index_key, new[name] or pk[name] or "*")
      if old then
        tinsert(old_index_key, old[name] or pk[name] or "*")
      end
    end)
    index_key = tconcat(index_key, ":")
    fun(index, { index_key, old and tconcat(old_index_key, ":") or index_key }, index.obsolete)
  end)
end

local function decode_indexes(self, db)
  local fields_indexed = self.fields_indexed

  local values = {}
  for i=1,#fields_indexed
  do
    local dbfield = fields_indexed[i]
    values[dbfield] = db[i]
  end

  return from_db(self.fields, values)
end

local function get_indexes_values(self, red, key)
  local fields_indexed = self.fields_indexed
  if #fields_indexed == 0 then
    return
  end

  local db = assert(red:hmget(key, unpack(fields_indexed)))
  if db[1] == ngx_null then
    return ngx_null
  end

  return decode_indexes(self, db)
end

function cache_class:set_unsafe(data, o)
  if not data then
    return nil, "bad args: data required"
  end

  local ok, err = check_pk(self.fields, data)
  if not ok then
    return nil, err
  end

  pcall(cache_desc_fixup, self)

  o = o or {}
  local ttl = o.ttl

  local set = function(red)
    local reason, old, old_ttl, old_expires
    local pk, key = self:make_pk(data)

    -- get exists data
    red:init_pipeline()

    red:hgetall(key)
    red:zscore(self.PK, key)

    local results = assert(red:commit_pipeline())

    old, old_expires = unpack(results)

    old = redis.get_row_result(old)
    if old ~= ngx_null and #old ~= 0 then
      old_expires = redis.get_row_result(old_expires)
      old_ttl = (old_expires ~= ngx_null and old_expires ~= "inf") and tonumber(old_expires) - now() or nil
      if old_ttl and old_ttl < 0 then
        old, old_ttl = nil, nil
      end
    else
      old, old_expires = nil, nil
    end

    if old then
      if not o.overwrite and not o.update then
        return ngx_null, ngx_null
      end
    else
      if o.update then
        return ngx_null
      end
    end

    local overwrite = old and o.overwrite

    local ok, err = check_in(self, data, old and not overwrite)
    if not ok then
      return nil, err
    end

    -- remove pk fields
    foreachi(self.fields, function(field) if field.pk then data[field.name] = nil end end)

    local update_data
    local i_crc32

    local on_new     = o.on_new     or fake_fun
    local on_nothing = o.on_nothing or fake_fun
    local on_update  = o.on_update  or fake_fun

    if old then
      old = red:array_to_hash(old)
      i_crc32 = old["#i"]
      old = from_db(self.fields, old)
      check_simple_types_out(self.fields, old)
      local eq = equals(old, data)
      update_data = (overwrite and not eq) or not eq or not compare(old, data)
      reason = update_data and { desc = overwrite and "overwrite()" or "update()", fun = on_update } or { desc = "nothing()" }
      self:debug("set()", function()
        return "overwrite_data=", overwrite and "Y" or "N", " update_data=", update_data and "Y" or "N",
               " old=", json_encode(old), " new=", json_encode(data)
      end)
      overwrite = overwrite and not eq
    else
      reason = { desc = "set()", fun = on_new }
      defaults(self.fields, data)
      update_data = true
    end

    ttl = old_ttl or ttl or self.ttl

    local key_part = self:make_key(pk)

    red:init_pipeline()

    red:multi()

    -- update indexes

    if #self.indexes ~= 0 then
      if self.i_crc32 ~= i_crc32 then
        self:debug("update_index()", function()
          return "#i=", i_crc32, "->", self.i_crc32
        end)
      end
      with_indexes(self, pk, data, old, function(index, index_pair, obsolete)
        local index_key, old_index_key = unpack(index_pair)
        if not obsolete and (not old or index_key ~= old_index_key) then
          -- new key or index field changed
          self:debug("update_index()", function()
            return "index=", cjson.encode(index), " add index_key=", index_key, " id=", key_part
          end)
          red:sadd(self.cache_id .. ":" .. index_key, key_part)
          reason = not reason.fun and { desc = "update()", fun = on_update } or reason
        end
        if obsolete or old_index_key ~= index_key then
          -- remove obsolete data
          red:srem(self.cache_id .. ":" .. old_index_key, key_part)
          self:debug("update_index()", function()
            return "index=", cjson.encode(index), " remove index_key=", old_index_key, " id=", key_part
          end)
          reason = not reason.fun and { desc = "update()", fun = on_update } or reason
        end
      end)
    end

    local db_data_set, db_data_del = to_db(self, data)

    if overwrite then
      -- overwrite key data if any
      red:del(key)
      old = nil
    else
      -- delete fields
      foreachi(db_data_del, function(todel)
        local dbfield, complex = unpack(todel)
        if complex then
          -- list or set
          red:del(key .. ":" .. dbfield)
        end
        red:hdel(key, dbfield)
      end)
    end

    -- update sets & lists

    local function update_complex(field, fun)
      local value, key_f = db_data_set[field.dbfield], key .. ":" .. field.dbfield
      if not value then
        return
      end

      reason = not reason.fun and { desc = "update()", fun = on_update } or reason

      if type(value) == "table" then
        red:del(key_f)
        foreachi(value, function(v) fun(key_f, v) end)
      else
        fun(key_f, value)
      end

      if ttl and #self.indexes == 0 then
        red:expire(key_f, ttl)
      end

      db_data_set[field.dbfield] = "ref"
    end

    foreachi(self.sets, function(field)
      update_complex(field, function(k, v)
        red:sadd(k, v)
      end)
    end)

    foreachi(self.sets, function(field)
      update_complex(field, function(k, v)
        red:rpush(k, v)
      end)
    end)

    self:debug(reason.desc, function()
      return "pk=", json_encode(pk), " ttl=", ttl, " data=", json_encode(data)
    end)

    -- may be nothing ?

    if not reason.fun then
      red:cancel_pipeline()
      -- restore pk fields
      foreach(pk, function(k,v) data[k] = v end)
      on_nothing(pk)
      return true
    end

    -- update key

    if update_data then
      red:hmset(key, db_data_set)
    end

    if not old then
      -- update primary index and ttl
      if ttl then
        red:zadd(self.PK, time() + ttl, key_part)
        -- DO NOT setup ttl - need for remove indexes
        if #self.indexes == 0 then
          red:expire(key, ttl)
        end
      else
        red:zadd(self.PK, "+inf", key_part)
      end
    end

    red:exec()

    if self.wait then
      red:wait(self.wait.slaves or 1,
               self.wait.ms or 100)
    end

    results = assert(red:commit_pipeline())

    ok, err = redis.check_pipeline(results)
    if not ok then
      self:warn(reason.desc, function()
        return err
      end)
    end

    if ok and not o.skip_log then
      local log_data
      if self.log_data then
        log_data = {}
        foreachi(self.fields, function(field)
          if not field.nostore then
            log_data[field.name] = data[field.name]
          end
        end)
      end
      self:log_event(old and events.UPDATE or events.SET, pk, log_data, ttl)
    end

    if ok then
      reason.fun()
      err = nil
    end

    -- restore pk fields
    foreach(pk, function(k,v) data[k] = v end)

    return ok, err
  end

  local start = now()
  ok, err = self.redis:handle(set, self.redis_rw)
    self:debug("set()", function()
    return "time=", now() - start
  end)

  return ok, err
end

function cache_class:set(data, o)
  return safe_call(self.set_unsafe, self, data, o)
end

function cache_class:delete_unsafe(pk, data, skip_log)
  if not pk then
    return nil, "bad args: pk required"
  end

  local ok, err = check_pk(self.fields, pk)
  if not ok then
    return nil, err
  end

  pcall(cache_desc_fixup, self)

  local delete = function(red)
    self:debug("delete()", function()
      return "pk=", json_encode(pk)
    end)

    local key, key_part

    _, key = self:make_pk(pk)
    key_part = self:make_key(pk)

    local exists = get_indexes_values(self, red, key) or (
      assert(red:exists(key)) == 1 and {} or ngx_null
    )
    if exists == ngx_null then
      -- not found
      return ngx_null
    end

    red:init_pipeline()

    red:multi()

    red:del(key)

    if next(exists) then
      with_indexes(self, {}, exists, nil, function(_, index_pair)
        local index_key = unpack(index_pair)
        if index_key then
          red:srem(self.cache_id .. ":" .. index_key, key_part)
        end
      end)
    end

    local key_part = self:make_key(pk)
    red:zrem(self.PK, key_part)
    foreachi(self.sets, function(field)
      red:del(key .. ":" .. field.dbfield)
    end)
    foreachi(self.lists, function(field)
      red:del(key .. ":" .. field.dbfield)
    end)

    red:exec()

    if self.wait then
      red:wait(self.wait.slaves or 1,
               self.wait.ms or 100)
    end

    local results = assert(red:commit_pipeline())

    local ok, err = redis.check_pipeline(results)
    if not ok then
      self:warn("delete_unsafe()", function()
        return err
      end)
    end

    if skip_log then
      return true
    end

    self:log_event(events.DELETE, pk)

    return true
  end

  return self.redis:handle(delete, self.redis_rw)
end

function cache_class:delete(pk, data)
  return safe_call(self.delete_unsafe, self, pk, data)
end

function cache_class:log_event(ev, pk, data, ttl)
  return self.log:log_event { ev = ev, pk = pk, data = self.log_data and data or nil, ttl = ttl }
end

-- memory index helpers --------------------------------------------------------

local function add_to_index(self, pk, data, ttl)
  local memory = self.memory
  local dict = memory.dict
  local idx_keys = {}
  for i,index in ipairs(memory.indexes or {})
  do
    local idx_key = { "$i:" .. i }
    if pcall(foreachi, index, function(field)
      local p = data[field]
      assert(p, "break")
      tinsert(idx_key, p ~= ngx_null and p or "null")
    end) then
      idx_key = tconcat(idx_key, ":")
      tinsert(idx_keys, idx_key)
      if not dict:object_fun(idx_key, function(idx_data, flags)
        idx_data = idx_data or {}
        for j,k in ipairs(idx_data)
        do
          if self:make_key(k) == self:make_key(pk) then
            -- already exists
            goto done
          end
        end
        tinsert(idx_data, pk)
        self:debug("add_to_index()", function()
          return "idx_key=", idx_key, " pk=", json_encode(pk), " data=", json_encode(data), " ttl=", ttl, " index=", json_encode(idx_data)
        end)
:: done ::
        return idx_data, flags
      end, ttl) then
        -- cleanup
        foreachi(idx_keys, function(idx_key)
          dict:delete(idx_key)
        end)
        return nil, "no memory"
      end
    end
  end
  return idx_keys
end

local function delete_from_index(self, pk)
  local old, flags
  self.memory.dict:object_fun(self:make_key(pk), function(value, f)
    old, flags = value, f
    return nil, 0
  end)

  if not old then
    return
  end

  for i,index in ipairs(self.memory.indexes or {})
  do
    local idx_key = { "$i:" .. i }
    if pcall(foreachi, index, function(field)
      local p = old[field]
      assert(p, "break")
      tinsert(idx_key, p ~= ngx_null and p or "null")
    end) then
      idx_key = tconcat(idx_key, ":")
      self.memory.dict:object_fun(idx_key, function(idx_data, flags)
        if idx_data then
          for j,k in ipairs(idx_data)
          do
            if self:make_key(k) == self:make_key(pk) then
              -- remove from index
              tremove(idx_data, j)
              self:debug("delete_from_index()", function()
                return "idx_key=", idx_key, " pk=", json_encode(pk), " index=", json_encode(idx_data)
              end)
              break
            end
          end
          return #idx_data ~= 0 and idx_data or nil, flags
        end
        return idx_data, flags
      end)
    end
  end

  return old, flags
end

-- memory index helpers end ----------------------------------------------------

local function memory_prefetch(self)
  local logid = self.log:log_id()

  if not self.memory.prefetch then
    self.memory.shm:set("logid", logid)
    return
  end

  local pk = self:make_pk { --[[ get all --]] }

  local on_set = self.memory.events.on_set
  local count = 0
  local start = now()

  local resp, err = self:get_unsafe(pk, self.redis_rw, function(data, key, ttl)
    pk = self:key2pk(key)
    local ok, err = self.memory.dict:object_add(key, data, ttl)
    if ok then
      -- add indexes
      ok, err = add_to_index(self, pk, data, ttl)
    end
    if not ok then
      self:warn("memory_prefetch()", function()
        return "please increase dictionary size"
      end)
      error(err)
    end
    count = count + 1
    on_set(pk, data, ttl)
  end)

  assert(resp, err)

  self.memory.shm:set("logid", logid)

  self:info("memory_prefetch()", function()
    return self.memory.name, " completed, count=", count, " at ", now() - start, " seconds"
  end)
end

local function do_memory_update(self, n)
  local logid_old = self.memory.shm:get("logid") or self.log:log_id()
  local log, err = self.log:get_events(logid_old, n)

  if not log then
    self:warn("memory_update()", function()
      return err
    end)
    return 0
  end

  if #log.events == 0 then
    self.memory.shm:set("logid", log.logid)
    return 0
  end

  self:debug("memory_update()", function()
    return "logid_old=", logid_old, ", logid=", log.logid
  end)

  local on_set, on_delete, on_purge =
    self.memory.events.on_set, self.memory.events.on_delete, self.memory.events.on_purge 

  local start = now()
  local added, updated, deleted = 0, 0, 0

  local dict = self.memory.dict
  local prefetch = self.memory.prefetch

  local tab_pk = {}
  local function flush_update()
    self:get_unsafe(tab_pk, self.redis_rw, function(data, key, ttl)
      local pk = self:key2pk(key)
      local old, flags = delete_from_index(self, pk)
      if old or prefetch then
        local nomemory
        if dict:object_add(key, data, ttl, flags) then
          if add_to_index(self, pk, data, ttl) then
            on_set(pk, data, ttl)
            if old then updated = updated + 1 else added = added + 1 end
          else
            dict:delete(key)
            nomemory = true
          end
        else
          nomemory = true
        end
        if nomemory then
          self:warn("memory_update()", function()
            return "please increase dictionary size"
          end)
        end
      end
    end)
    tab_pk = {}
  end

  foreachi(log.events, function(event)
    self:debug("memory_update()", function()
      return "event: ", json_encode(event)
    end)
    if event.ev == events.SET or event.ev == events.UPDATE then
      tinsert(tab_pk, event.pk)
    elseif event.ev == events.DELETE then
      flush_update()
      local data = delete_from_index(self, event.pk)
      if data then
        on_delete(event.pk, data)
        deleted = deleted + 1
      end
    elseif event.ev == events.PURGE then
      tab_pk = {}
      dict:flush_all()
      dict:flush_expired()
      on_purge()
    end
    self.memory.shm:set("logid", event.logid)
  end)

  flush_update()

  self:info("memory_update()", function()
    return "completed, added=", added, ", updated=", updated, ", deleted=", deleted, " at ", now() - start, " seconds"
  end)

  return #log.events
end

local function memory_update(self)
  repeat
    local count = do_memory_update(self, 1000)
  until count ~= 1000 or worker_exiting()
end

local function memory_cleanup(self)
  local start = now()
  local count = self.memory.dict:flush_expired()
  if count ~= 0 then
    self:info("memory_cleanup()", function()
      return "count=", count, " at ", now() - start, " seconds"
    end)
  end
end

local DDOS_FLAG = -1

local function get_memory(self, red, pk, callback)
  local memory = self.memory
  if not memory then
    return nil, nil, "only for in-memory caches"
  end

  local ok, err = check_pk(self.fields, pk)
  if not ok then
    return nil, nil, err
  end

  callback = callback or function() end

  local key, val, flags
  local memory_ttl = memory.ttl
  local dict = memory.dict

  if not memory_ttl or memory_ttl > 0 then
    key = self:make_key(pk)
    val, flags = unpack(callback and { dict:object_fun(key, function(val, flags)
      callback(val, flags)
      return val, flags
    end) } or { dict:object_get(key) })
    self:debug("get_memory()", function()
      return "lookup hot: pk=", json_encode(pk), " data=", val and json_encode(val) or "NOT_FOUND"
    end)
  end

  local minute = floor(time() / 60)

  if val then
    dict:incr("$h:" .. minute, 1, 0)
  else
    dict:incr("$m:" .. minute, 1, 0)
    self:get_unsafe(pk, red, function(data, key, ttl)
      if not memory_ttl or memory_ttl > 0 then
        local nomemory
        ttl = ttl and min(ttl, memory_ttl or ttl) or nil
        val, flags = dict:object_fun(key, function(_, flags)
          return data, flags
        end, ttl)
        if val then
          if add_to_index(self, pk, val, ttl) then
            self:debug("get_memory()", function()
              return "save hot: pk=", json_encode(pk), " data=", json_encode(val), " ttl=", ttl
            end)
          else
            dict:delete(key)
            nomemory = true
          end
        else
          nomemory = true
        end
        if nomemory then
          self:warn("memory_update()", function()
            return "please increase dictionary size"
          end)
        end
      end
      val = data
      callback(val)
    end)
  end

  if not val then
    if red == self.redis_rw then
      dict:object_set(key, ngx_null, self.memory.ddos_timeout or 1, DDOS_FLAG)
      flags = DDOS_FLAG
    end
    val = ngx_null
  end

  return val, flags
end

function cache_class:get_memory_slave(pk, callback)
  local data, flags, err = get_memory(self, self.redis_ro, pk, callback)
  if not data then
    return nil, err
  end
  if data ~= ngx_null or flags == DDOS_FLAG then
    return data, flags
  end
  -- failover on master node
  data, flags, err = get_memory(self, self.redis_rw, pk, callback)
  return data, data and flags or err
end

function cache_class:get_memory_master(pk, callback)
  local data, flags, err = get_memory(self, self.redis_rw, pk, callback)
  return data, data and flags or err
end

function cache_class:hits(backward, m)
  if not self.memory then
    return nil, "no in-memory data"
  end
  backward, m = backward or 1, m or 1
  local hits, miss = 0, 0
  local minute = floor(time() / 60) - backward
  for t = minute, minute + m
  do
    hits = hits + (self.memory.dict:get("$h:" .. t) or 0)
    miss = miss + (self.memory.dict:get("$m:" .. t) or 0)  
  end
  return hits, miss
end

function cache_class:memory_scan(fun)
  foreachi(self.memory.dict:get_keys(0), function(key)
    if not key:match("^%$") then
      local data = self.memory.dict:object_get(key)
      if type(data) == "table" then
        fun(self:key2pk(key), data)
      end
    end
  end)
end

function cache_class:get_by_index(data, o)
  assert(self.memory and self.memory.prefetch,
         "index operation is possible only with in-memory caches with prefetch")

  o = o or {}

  local callback, filter = o.callback, o.filter or function() return true end
  local pk, flags
  local dict = self.memory.dict

  for i,index in ipairs(self.memory.indexes or {})
  do
    local idx_key = { "$i:" .. i }
    if pcall(foreachi, index, function(field)
      local p = data[field]
      assert(p, "break")
      tinsert(idx_key, p ~= ngx_null and p or "null")
    end) then
      pk = dict:object_get(tconcat(idx_key, ":"))
      if pk then
        break
      end
    end
  end

  if not pk then
    return ngx_null
  end

  local items = {}

  foreachi(pk, function(k)
    local key = self:make_key(k)
    local data, flags = unpack(callback and { dict:object_fun(key, function(value, flags)
      callback(k, value, flags)
      return value, flags
    end) } or { dict:object_get(key) })
    local pk = self:key2pk(key)
    if filter(pk, data, flags) then
      tinsert(items, { pk, data, flags })
    end
  end)

  return #items ~= 0 and items or ngx_null
end

local function purge_by_chunk(self, red, index, next_keys_fn)
  local count = 0

  while not worker_exiting()
  do
    local start = now()
    local keys = next_keys_fn()
    if not next(keys) then
      break
    end

    -- collect index keys
    local bulk = {}
    local fields_indexed = self.fields_indexed
    local index_keys = #fields_indexed ~= 0 and {} or nil

    if index_keys then
      red:init_pipeline()
    end

    foreach(keys, function(key_part)
      tinsert(bulk, key_part)
      if index_keys then
        red:hmget(self.cache_id .. ":" .. key_part, unpack(fields_indexed))
      end
    end)

    if index_keys then
      foreachi(assert(red:commit_pipeline()), function(row)
        local index_values = redis.get_row_result(row)
        local index_keys_k = {}
        with_indexes(self, {}, decode_indexes(self, index_values), nil, function(_, index_pair)
          local index_key = unpack(index_pair)
          tinsert(index_keys_k, index_key)
        end)
        tinsert(index_keys, index_keys_k)
      end)
    end

    red:init_pipeline()

    -- flush data
    local n = 0

    foreachi(bulk, function(key_part)
      red:multi()

      local key = self.cache_id .. ":" .. key_part

      -- remove from XX:keys
      red:zrem(index, key_part)

      -- remove from XX:key:N set
      foreachi(self.sets, function(field)
        red:del(key .. ":" .. field.dbfield)
      end)

      -- remove from XX:key:N list
      foreachi(self.lists, function(field)
        red:del(key .. ":" .. field.dbfield)
      end)

      if index_keys then 
        -- remove indexes
        foreachi(index_keys[1 + n], function(index_key)
          red:srem(self.cache_id .. ":" .. index_key, key_part)
        end)
      end

      red:del(key)
      red:exec()

      n = n + 1
    end)

    if self.wait then
      red:wait(self.wait.slaves or 1,
               self.wait.ms or 10000)
    end

    assert(red:commit_pipeline())

    if n ~= 0 then
      self:info("purge_chunk()", function()
        return "count=", n, " at ", now() - start, " seconds"
      end)
      count = count + n
    end
  end

  return count
end

local function purge_keys(self, red)
  local lock = self.redis:create_lock(self.cache_id, 10)
  if not lock:aquire() then
    self:warn("purge()", function()
      return "can't aquire the lock ..."
    end)
    return nil, "locked"
  end

  local start = now()

  local old = self.cache_id .. ":old"

  local count = 0
  local cursor = "0"

  repeat
    local renamed, err = red:renamenx(self.PK, old)
    assert(renamed or err:match("no such key"), err)
    count = count + purge_by_chunk(self, red, old, function()
      local keys, err
      repeat
        local tmp = assert(red:zscan(old, cursor, "count", 1000, "match", "*"))
        cursor, keys, err = unpack(tmp)
        assert(not err, err)
        lock:prolong()
      until cursor == "0" or #keys ~= 0
      return red:array_to_hash(keys)
    end)
  until renamed == 1 or err:match("no such key")

  if cursor == "0" then
    -- full
    assert(red:del(old))
  end

  lock:release()

  if count ~= 0 then
    self:info("purge()", function()
      return "count=", count, " at ", now() - start, " seconds"
    end)
  end

  return true
end

local function cleanup_keys(self, red)
  local lock = self.redis:create_lock(self.cache_id, 10)
  if not lock:aquire() then
    return nil, "can't aquire the lock"
  end

  local start = now()

  local count = purge_by_chunk(self, red, self.PK, function()
    local keys = assert(red:zrangebyscore(self.PK, 0, time(), "LIMIT", 0, 1000))
    lock:prolong()
    local h = {}
    foreachi(keys, function(k) h[k] = true end)
    return h
  end)

  lock:release()

  if count ~= 0 then
    self:info("cleanup()", function()
      return "count=", count, " at ", now() - start, " seconds"
    end)
  end

  return true
end

function cache_class:purge(max_wait)
  pcall(cache_desc_fixup, self)

  local function purge()
    return self.redis:handle(function(red)
      local ok, err = purge_keys(self, red)
      if not ok then
        return nil, err
      end
      red:del("L:" .. self.cache_name .. ":last_modified")
      return self:log_event(events.PURGE)
    end, self.redis_rw)
  end

  local completed

  local purge_job
  purge_job = job.new("purge " .. self.cache_name, function()
    local ok, ret, err = pcall(purge)
    if ok and ret then
      purge_job:stop()
      purge_job:clean()
      completed = true
    elseif err ~= "locked" then
      self:err("purge()", function()
        return err or ret
      end)
    end
    return true
  end, 1)

  if purge_job:running() then
    return nil, "already in progress"
  end

  purge_job:run()

  local wait_to = now() + (max_wait or 0)

  while not completed and now() < wait_to and not worker_exiting() do
    sleep(0.1)
  end

  return completed and true or "async"
end

function cache_class:last_modified()
  local last_modified = function(red)
    return assert(red:get("L:" .. self.cache_name .. ":last_modified"))
  end
  return self.redis:handle(last_modified, self.redis_rw)
end

function cache_class:update_last_modified(lm)
  local update_last_modified = function(red)
    assert(red:set("L:" .. self.cache_name .. ":last_modified", lm))
    self:info("update_last_modified()", function()
      return lm
    end)
    return true
  end
  return self.redis:handle(update_last_modified, self.redis_rw)
end

function cache_class:create_lock(name, period)
  return self.redis:create_lock(name, period)
end

local batch_class = {}

function batch_class:add(f, ...)
  local fun = self.cache[f]
  if not fun then
    return nil, f .. " function is not found"
  end

  tinsert(self.batch, { function(...)
    assert(fun(...))
  end, { ... } })

  if #self.batch < self.size then
    return true
  end

  return self:flush()
end

function batch_class:flush()
  local threads = {}

  local ok, err = pcall(foreachi, self.batch, function(opts)
    local fun, args = unpack(opts)
    local thr = thread_spawn(fun, self.cache, unpack(args))
    assert(thr, "failed to create corutine")
    tinsert(threads, thr)
  end)

  if not ok then
    self.cache:err("batch:flush()", function()
      return err
    end)
  end

  for i=1,#threads
  do
    local result = { thread_wait(threads[i]) }
    local ok = tremove(result, 1)
    self.ret[1 + #self.ret] = { ok, result, tremove(self.batch, 1)[2] }
  end

  return ok, err
end

function batch_class:results()
  return self.ret
end

function batch_class:cancel()
  self.batch = {}
  self.ret = {}
end

function cache_class:create_batch(size)
  return setmetatable({
    size = size,
    threads = {},
    batch = {},
    ret = {},
    cache = self
  }, { __index = batch_class })
end

function cache_class:ro_socket()
  return self.redis_ro
end

function cache_class:rw_socket()
  return self.redis_rw
end

local function init_memory(self)
  if not self.memory then
    return
  end

  if self.memory.prefetch then
    self.memory.ttl = nil -- no ttl
  end

  self.memory.events = self.memory.events or {}

  self.memory.events.on_set    = self.memory.events.on_set    or fake_fun
  self.memory.events.on_delete = self.memory.events.on_delete or fake_fun
  self.memory.events.on_purge  = self.memory.events.on_purge  or fake_fun


  -- fix worker local ttl
  self.memory.L1 = self.memory.L1 or { ttl = 0, count = 0 }
  self.memory.L1.ttl = min(self.memory.L1.ttl or 0,
                           self.memory.ttl or self.memory.L1.ttl or 0)

  local shdict = require "shdict"
  local shdict_ex = require "shdict_ex"

  local err
  self.memory.shm = shdict.new(self.memory.name)
  self.memory.dict, err = assert(shdict_ex.new(self.memory.name,
                                               self.memory.L1.ttl,
                                               self.memory.L1.ttl ~= 0 and self.memory.L1.count or 0))
end

function cache_class:init()
  cache_desc_fixup(self)

  if self.ttl then
    job.new("cleanup " .. self.cache_name, function()
      self.redis:handle(function(red)
        local ok, err = pcall(cleanup_keys, self, red)
        if not ok then
          self:err("cleanup_keys()", function()
            return err
          end)
        end
      end, self.redis_rw)
      return true
    end, 10):run()
  end

  local update_job = job.new("memory update " .. self.cache_name, function()
    memory_update(self)
    return true
  end, 1)

  job.new("memory cleanup " .. self.cache_name, function()
    memory_cleanup(self)
    return true
  end, 60):run()

  if not self.memory.prefetch then
    job.new("memory hits " .. self.cache_name, function()
      local p = max(60, self.memory.ttl) / 60
      local hits_avg, miss_avg = self:hits(p, p)
      local hits_1, miss_1 = self:hits(1, 1)
      if hits_1 + miss_1 == 0 then
        return
      end
      self:info("memory_hits()", function()
        return "avg: ", floor(100 * hits_avg / (hits_avg + miss_avg)), "%, ",
               "last: ", floor(100 * hits_1 / (hits_1 + miss_1)), "% ",
               "hits=", hits_1, " misses=", miss_1
      end)
      return true
    end, 60):run()
  end

  local prefetch_job
  prefetch_job = job.new("memory prefetch " .. self.cache_name, function()
    local ok, err = pcall(memory_prefetch, self)
    if ok then
      prefetch_job:stop()
    else
      self:err("memory_prefetch()", function()
        return err
      end)
    end
    return true
  end, 1)

  update_job:wait_for(prefetch_job)

  prefetch_job:run()
  update_job:run()

  return prefetch_job
end

function cache_class:desc()
  local get_cache_desc = function(red)
    local cache_desc = assert(red:hgetall("cache:" .. self.cache_name))
    if cache_desc == ngx_null then
      return
    end

    cache_desc = red:array_to_hash(cache_desc)
    cache_desc.fields = json_decode(cache_desc.fields)
    cache_desc.indexes = json_decode(cache_desc.indexes)
    cache_desc.f_index = nil
    cache_desc.i_index = nil

    return cache_desc
  end

  return system:handle(get_cache_desc)
end

-- public api

function _M.new(opts, redis_opts)
  assert(opts.fields and type(opts.fields) == "table", "fields table required")
  assert(opts.cache_id, "cache_id required")
  assert(opts.cache_name, "cache_name required")

  opts.redis = redis.new(redis_opts)
  opts.redis_ro = opts.redis:ro_socket()
  opts.redis_rw = opts.redis:rw_socket()

  opts.sets = {}
  opts.lists = {}

  opts.indexes = opts.indexes or {}
  for i, index in ipairs(opts.indexes)
  do
    opts.indexes[i] = { fields = index }
    foreachi(index, function(name)
      local field = unpack(find_if_i(opts.fields, function(field)
        return field.name == name
      end) or {})
      assert(field, "invalid index: field " .. name .. " is not found")
      field.indexed = true
    end)
  end

  foreachi(opts.fields, function(field)
    field.name = field.name:lower()
    if field.ftype == ftype.SET then
      tinsert(opts.sets, field)
    end
    if field.ftype == ftype.LIST then
      tinsert(opts.lists, field)
    end
    if field.skip then
      field.nostore = true
    end
  end)

  local function setpartof(t)
    foreachi(t, function(field)
      foreachi(type(field.source) == "table" and field.source or { field.source }, function(source)
        for i=1,#opts.fields
        do
          if opts.fields[i].name == source then
            opts.fields[i].skip = true
            opts.fields[i].partof = field.name
            break
          end
        end
      end)
    end)
  end

  setpartof(opts.sets)
  setpartof(opts.lists)

  opts.PK = opts.cache_id .. ":keys"

  local CONFIG = ngx.shared.config

  local scope = redis_opts.scope or CONFIG:get("ngx.caches.scope") or "ngx" 

  opts.debug_enabled = CONFIG:get(scope .. ".caches." .. opts.cache_name .. ".debug")

  local config_ttl = CONFIG:get(scope .. ".caches." .. opts.cache_name .. ".ttl")
  if not opts.ttl then
    opts.ttl = config_ttl
  end
  if opts.ttl then
    opts.ttl = opts.ttl * 60
  end

  local cache = assert(setmetatable(opts, { __index = cache_class }))

  opts.log = log_server.new(cache)

  init_memory(cache)

  return cache
end

do
  common.export_to(_M)
  _M.events = events
end

return _M