-- Atomic check-and-reserve across ACCOUNT and GROUP limits.
-- Redis runs the whole script without interleaving any other command, so two
-- concurrent checks can never both see the same headroom.
--
-- KEYS[1] group limit        lim:{G}:grp
-- KEYS[2] account limit      lim:{G}:acct:{A}
-- KEYS[3] reservation        rsv:{G}:{orderId}
-- KEYS[4] pending set        rsv:{G}:pending
-- ARGV[1] orderId  ARGV[2] amountCents  ARGV[3] requestHash  ARGV[4] nowMillis
-- ARGV[5] accountId  ARGV[6] groupId  ARGV[7] terminalTtlSeconds
--
-- Returns {status, detail, accountHeadroomCents, groupHeadroomCents, <reservation hash pairs>}
-- status: APPROVED | DECLINED | REPLAY | CONFLICT | NO_LIMIT

local now = tonumber(ARGV[4])

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

local function reply(status, detail, acctHr, grpHr)
  local out = {status, detail or '', fmt(acctHr), fmt(grpHr)}
  local h = redis.call('HGETALL', KEYS[3])
  for i = 1, #h do out[#out + 1] = h[i] end
  return out
end

-- Idempotency: the order ID is the key. Same payload replays, different payload conflicts.
local existing = redis.call('HGET', KEYS[3], 'requestHash')
if existing then
  if existing == ARGV[3] then
    return reply('REPLAY', redis.call('HGET', KEYS[3], 'failedLevel') or '', headroom(KEYS[2]), headroom(KEYS[1]))
  end
  return reply('CONFLICT', '', nil, nil)
end

local amt = tonumber(ARGV[2])
local acctHr = headroom(KEYS[2])
local grpHr = headroom(KEYS[1])
if acctHr == nil or grpHr == nil then
  return {'NO_LIMIT', acctHr == nil and 'ACCOUNT' or 'GROUP', '', ''}
end

local failed = nil
if amt > acctHr then failed = 'ACCOUNT' elseif amt > grpHr then failed = 'GROUP' end

if failed then
  redis.call('HSET', KEYS[3],
    'orderId', ARGV[1], 'accountId', ARGV[5], 'groupId', ARGV[6],
    'state', 'DECLINED', 'failedLevel', failed,
    'amountCents', amt, 'filledCents', 0, 'pendingAmendCents', 0,
    'lastEventSeq', 0, 'version', 1, 'requestHash', ARGV[3], 'createdTs', ARGV[4])
  redis.call('EXPIRE', KEYS[3], tonumber(ARGV[7]))
  return reply('DECLINED', failed, acctHr, grpHr)
end

redis.call('HINCRBY', KEYS[2], 'reserved', amt)
redis.call('HINCRBY', KEYS[1], 'reserved', amt)
redis.call('HSET', KEYS[3],
  'orderId', ARGV[1], 'accountId', ARGV[5], 'groupId', ARGV[6],
  'state', 'RESERVED', 'failedLevel', '',
  'amountCents', amt, 'filledCents', 0, 'pendingAmendCents', 0,
  'lastEventSeq', 0, 'version', 1, 'requestHash', ARGV[3], 'createdTs', ARGV[4])
redis.call('ZADD', KEYS[4], now, ARGV[1])
return reply('APPROVED', '', acctHr - amt, grpHr - amt)
