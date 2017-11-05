local job = require "job"

local example = require "caches.example"
local example_upsync = require "caches.example_upsync"

local function startup()
  local init_job
  init_job = job.new("ngx caches init", function()
    example.init()
    example_upsync.init()
    init_job:stop()
    return true
  end, 1)

  init_job:clean()
  init_job:run()
end

return startup