local http = require 'resty.http'
local semaphore = require 'ngx.semaphore'
local gw = require 'gateways'

local method = ngx.req.get_method()
if method ~= 'GET' and method ~= 'HEAD' then
  ngx.status = 405
  ngx.header['Allow'] = 'GET, HEAD'
  return ngx.say('Method Not Allowed')
end

local request_path = ngx.var.request_uri
local gateways = gw.get_gateways()
local timeouts = gw.get_timeouts()
local debug = gw.is_debug()
local fallback_error = gw.fallback_error()
local mime_prefix = gw.match_mime_prefix()

local first_sem = semaphore.new(0)
local winner_slot = { picked = false, result = nil }
local first_error = nil

local function log_debug(msg)
  if debug then ngx.log(ngx.INFO, msg) end
end

local function is_mime_accepted(ct)
  if not ct then return false end
  return ct:lower():find(mime_prefix, 1, true) == 1
end

local function fetch_one(host, delay_ms)
  if delay_ms and delay_ms > 0 then ngx.sleep(delay_ms / 1000) end

  local httpc = http.new()
  httpc:set_timeouts(timeouts.connect_timeout_ms, 1000, timeouts.read_timeout_ms)

  log_debug('tcp connect ' .. host .. ':443')
  local ok, err = httpc:connect(host, 443)
  if not ok then
    log_debug('connect failed ' .. host .. ': ' .. (err or 'nil'))
    return
  end

  log_debug('tls handshake ' .. host)
  local sh_ok, sh_err = httpc:ssl_handshake(nil, host, timeouts.ssl_verify)
  if not sh_ok then
    log_debug('handshake failed ' .. host .. ': ' .. (sh_err or 'nil'))
    pcall(function() httpc:close() end)
    return
  end

  local headers = {
    Host = host,
    Range = ngx.var.http_range,
    ['If-None-Match'] = ngx.var.http_if_none_match,
    ['If-Modified-Since'] = ngx.var.http_if_modified_since,
    ['Accept-Encoding'] = ngx.var.http_accept_encoding,
    ['User-Agent'] = ngx.var.http_user_agent,
    Accept = mime_prefix .. "*"
  }

  local res, req_err = httpc:request({ method = method, path = request_path, headers = headers })
  if not res then
    log_debug('request failed ' .. host .. ': ' .. (req_err or 'nil'))
    httpc:close()
    return
  end

  local status = res.status or 0
  local ct = res.headers and res.headers["Content-Type"] or res.headers and res.headers["content-type"]
  log_debug('status ' .. host .. ': ' .. tostring(status) .. ' ct=' .. tostring(ct))

  if status >= 200 and status < 400 and is_mime_accepted(ct) then
    if not winner_slot.picked then
      winner_slot.picked = true
      winner_slot.result = { httpc = httpc, res = res, host = host }
      first_sem:post(1)
      return
    end
  else
    if not first_error then
      local body = res:read_body()
      first_error = { status = status, headers = res.headers, body = body }
    end
    pcall(function() httpc:close() end)
  end
end

local threads = {}
for i, host in ipairs(gateways) do
  local delay_ms = (i - 1) * (timeouts.delay_ms or 0)
  threads[#threads + 1] = ngx.thread.spawn(fetch_one, host, delay_ms)
end

local ok, perr = first_sem:wait(5)
if not ok then
  log_debug('no winner within timeout: ' .. (perr or 'nil'))
  for _, th in ipairs(threads) do pcall(ngx.thread.kill, th) end
  if fallback_error and first_error then
    for k, v in pairs(first_error.headers or {}) do
      local kl = type(k) == 'string' and k:lower() or ''
      if type(v) == 'table' then v = v[1] end
      if kl ~= 'transfer-encoding' then ngx.header[k] = v end
    end
    ngx.status = first_error.status or 502
    if first_error.body then ngx.print(first_error.body) end
    return
  end
  return ngx.exit(502)
end

for i, th in ipairs(threads) do pcall(ngx.thread.kill, th) end
local win = winner_slot.result

local res = win.res
for k, v in pairs(res.headers) do
  local kl = string.lower(k)
  if type(v) == 'table' then v = v[1] end
  if kl ~= 'transfer-encoding' then ngx.header[k] = v end
end
ngx.status = res.status

local reader = res.body_reader
if not reader then
  local body = res:read_body()
  if body then ngx.print(body) end
  win.httpc:set_keepalive()
  return
end

while true do
  local chunk, err = reader(8192)
  if err then break end
  if not chunk then break end
  ngx.print(chunk)
  ngx.flush(true)
end

win.httpc:set_keepalive()
