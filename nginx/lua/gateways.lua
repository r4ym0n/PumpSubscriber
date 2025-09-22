local M = {}

local function split_csv(value)
  local list = {}
  if not value or value == '' then return list end
  for token in string.gmatch(value, '([^,]+)') do
    local trimmed = token:match('^%s*(.-)%s*$')
    if trimmed ~= '' then table.insert(list, trimmed) end
  end
  return list
end

function M.get_gateways()
  local env_val = os.getenv('IPFS_GATEWAYS')
  local list = split_csv(env_val)
  if #list == 0 then
    list = {
      'gateway.pinata.cloud',
      'ipfs.io',
      'dweb.link',
      'trustless-gateway.link'
    }
  end
  return list
end

local function to_number(name, default)
  local v = tonumber(os.getenv(name))
  if not v or v <= 0 then return default end
  return v
end

local function to_bool(name, default)
  local v = os.getenv(name)
  if v == nil then return default end
  v = v:lower()
  if v == '1' or v == 'true' or v == 'yes' or v == 'on' then return true end
  if v == '0' or v == 'false' or v == 'no' or v == 'off' then return false end
  return default
end

function M.get_timeouts()
  return {
    connect_timeout_ms = to_number('IPFS_CONNECT_TIMEOUT_MS', 500),
    read_timeout_ms    = to_number('IPFS_READ_TIMEOUT_MS', 4000),
    delay_ms           = to_number('IPFS_DELAY_MS', 0),
    ssl_verify         = to_bool('IPFS_SSL_VERIFY', true),
  }
end

function M.is_debug()
  return to_bool('IPFS_DEBUG', false)
end

function M.fallback_error()
  return to_bool('IPFS_FALLBACK_ERROR', false)
end

function M.match_mime_prefix()
  return os.getenv('IPFS_MATCH_MIME_PREFIX') or 'image/'
end

return M
