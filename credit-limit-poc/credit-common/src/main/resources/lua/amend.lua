-- Pre-check an order amend. Only the increase (delta) needs headroom.
-- Amend-up: reserve the delta now, before the gateway sends the amend to the venue.
-- Amend-down: approve without releasing anything; exposure is released only when the
-- venue confirms the smaller order (AMEND_CONFIRMED event).
--
-- KEYS[1] group limit  KEYS[2] account limit  KEYS[3] reservation
-- ARGV[1] amendId  ARGV[2] newTotalAmountCents  ARGV[3] nowMillis
--
-- Returns {status, detail, accountHeadroomCents, groupHeadroomCents, <reservation hash pairs>}
-- status: APPROVED | DECLINED | REPLAY | UNKNOWN_ORDER | ORDER_CLOSED | AMEND_PENDING
-- detail: UP | DOWN | NONE for APPROVED, failed level for DECLINED

local now = tonumber(ARGV[3])

local function effective_limit(key)
  local v = redis.call('HMGET', key, 'limit', 'overrideLimit', 'overrideExpiry')
  if not v[1] then return nil end
  local limit = tonumber(v[1])
  if v[2] and v[3] and tonumber(v[3]) > now then limit = tonumber(v[2]) end
  return limit
end

local function headroom(key)
  local limit = effective_limit(key)
  if not limit then return nil end
  local u = redis.call('HMGET', key, 'used', 'reserved')
  return limit - tonumber(u[1] or 0) - tonumber(u[2] or 0)
end

local function fmt(x)
  if x == nil then return '' end
  return string.format('%d', x)
end

local function reply(status, detail)
  local out = {status, detail or '', fmt(headroom(KEYS[2])), fmt(headroom(KEYS[1]))}
  local h = redis.call('HGETALL', KEYS[3])
  for i = 1, #h do out[#out + 1] = h[i] end
  return out
end

local s = redis.call('HMGET', KEYS[3], 'state', 'amountCents', 'pendingAmendCents', 'lastAmendId', 'lastAmendResult')
local state = s[1]
if not state then return {'UNKNOWN_ORDER', '', '', ''} end
if s[4] == ARGV[1] then return reply('REPLAY', s[5]) end
if state ~= 'RESERVED' and state ~= 'WORKING' and state ~= 'PARTIALLY_FILLED' then
  return reply('ORDER_CLOSED', state)
end
if tonumber(s[3] or 0) > 0 then return reply('AMEND_PENDING', '') end

local amount = tonumber(s[2])
local delta = tonumber(ARGV[2]) - amount

if delta <= 0 then
  local d = delta < 0 and 'DOWN' or 'NONE'
  redis.call('HSET', KEYS[3], 'lastAmendId', ARGV[1], 'lastAmendResult', d)
  redis.call('HINCRBY', KEYS[3], 'version', 1)
  return reply('APPROVED', d)
end

local failed = nil
if delta > headroom(KEYS[2]) then failed = 'ACCOUNT' elseif delta > headroom(KEYS[1]) then failed = 'GROUP' end
if failed then
  redis.call('HSET', KEYS[3], 'lastAmendId', ARGV[1], 'lastAmendResult', failed)
  redis.call('HINCRBY', KEYS[3], 'version', 1)
  return reply('DECLINED', failed)
end

redis.call('HINCRBY', KEYS[2], 'reserved', delta)
redis.call('HINCRBY', KEYS[1], 'reserved', delta)
redis.call('HINCRBY', KEYS[3], 'amountCents', delta)
redis.call('HSET', KEYS[3], 'pendingAmendCents', delta, 'lastAmendId', ARGV[1], 'lastAmendResult', 'UP')
redis.call('HINCRBY', KEYS[3], 'version', 1)
return reply('APPROVED', 'UP')
