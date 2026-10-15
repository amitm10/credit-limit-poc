-- Apply one order lifecycle event to a reservation and to both limit levels, atomically.
--
-- KEYS[1] group limit  KEYS[2] account limit  KEYS[3] reservation  KEYS[4] pending set
-- ARGV[1] type: ACK | FILL | CANCEL | REJECT | AMEND_CONFIRMED | AMEND_REJECTED
-- ARGV[2] per-order event sequence number from the gateway
-- ARGV[3] amountCents: FILL = CUMULATIVE filled notional (like FIX CumQty x AvgPx),
--         AMEND_CONFIRMED = new total order notional, otherwise ignored
-- ARGV[4] nowMillis  ARGV[5] terminalTtlSeconds  ARGV[6] orderId
--
-- Idempotency:
--   * Non-fill events are deduplicated by sequence number (seq <= lastEventSeq is dropped).
--   * Fills carry cumulative amounts, so they are applied as max(filled, cum). A late or
--     duplicate fill can never double-count, and an out-of-order fill is never lost.
--
-- Returns {status, alert, accountHeadroomCents, groupHeadroomCents, <reservation hash pairs>}
-- status: APPLIED | DUPLICATE | IGNORED | UNKNOWN_ORDER | UNKNOWN_TYPE
-- alert:  '' | LATE_ACK | LATE_FILL | OVERFILL | UNCHECKED_INCREASE

local now = tonumber(ARGV[4])
local LIVE = {RESERVED = true, WORKING = true, PARTIALLY_FILLED = true}
local TERMINAL = {FILLED = true, CANCELLED = true, REJECTED = true}

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

local function adjust(field, delta)
  if delta ~= 0 then
    redis.call('HINCRBY', KEYS[2], field, delta)
    redis.call('HINCRBY', KEYS[1], field, delta)
  end
end

local s = redis.call('HMGET', KEYS[3], 'state', 'amountCents', 'filledCents', 'pendingAmendCents', 'lastEventSeq')
local state = s[1]
if not state then return {'UNKNOWN_ORDER', '', '', ''} end
if state == 'DECLINED' then return reply('IGNORED', 'DECLINED') end

local typ = ARGV[1]
local seq = tonumber(ARGV[2])
local amount = tonumber(s[2])
local filled = tonumber(s[3])
local pending = tonumber(s[4] or 0)
local lastSeq = tonumber(s[5] or 0)
local alert = ''

-- 1. Drop duplicates before touching anything.
if typ == 'FILL' then
  if tonumber(ARGV[3]) <= filled then return reply('DUPLICATE', 'STALE_FILL') end
elseif seq <= lastSeq then
  return reply('DUPLICATE', '')
end

-- 2. An ACK or FILL proves the order is live. If the sweeper already expired its
--    reservation, put the remaining exposure back (no limit check: the order exists).
if state == 'EXPIRED' and (typ == 'ACK' or typ == 'FILL') then
  adjust('reserved', math.max(amount - filled, 0))
  state = 'WORKING'
  alert = 'LATE_ACK'
end

-- 3. Apply the event.
if typ == 'ACK' then
  if not LIVE[state] then return reply('IGNORED', state) end
  if state == 'RESERVED' then state = 'WORKING' end

elseif typ == 'FILL' then
  local cum = tonumber(ARGV[3])
  local delta = cum - filled
  local fromReserved = 0
  if LIVE[state] then
    fromReserved = math.min(delta, math.max(amount - filled, 0))
  else
    -- The fill is real even though the order is closed: count it, and alert.
    alert = (state == 'FILLED') and 'OVERFILL' or 'LATE_FILL'
  end
  adjust('reserved', -fromReserved)
  adjust('used', delta)
  filled = cum
  if LIVE[state] then
    if filled >= amount then
      if filled > amount then alert = 'OVERFILL' end
      state = 'FILLED'
      pending = 0
    else
      state = 'PARTIALLY_FILLED'
    end
  end

elseif typ == 'CANCEL' or typ == 'REJECT' then
  local terminal = (typ == 'CANCEL') and 'CANCELLED' or 'REJECTED'
  if LIVE[state] then
    adjust('reserved', -math.max(amount - filled, 0))
  elseif state ~= 'EXPIRED' then
    return reply('IGNORED', state)
  end
  state = terminal
  pending = 0

elseif typ == 'AMEND_CONFIRMED' then
  if not LIVE[state] then return reply('IGNORED', state) end
  local target = math.max(tonumber(ARGV[3]), filled)
  if target < amount then
    adjust('reserved', -(amount - target))
  elseif target > amount then
    -- Venue confirmed a size we never pre-checked. The order exists, so count it and alert.
    adjust('reserved', target - amount)
    alert = 'UNCHECKED_INCREASE'
  end
  amount = target
  pending = 0
  if filled > 0 and filled >= amount then state = 'FILLED' end

elseif typ == 'AMEND_REJECTED' then
  if not LIVE[state] or pending <= 0 then return reply('IGNORED', state) end
  local release = math.min(pending, math.max(amount - filled, 0))
  adjust('reserved', -release)
  amount = amount - pending
  pending = 0

else
  return {'UNKNOWN_TYPE', typ, '', ''}
end

-- 4. Persist the new reservation state.
redis.call('HSET', KEYS[3],
  'state', state, 'amountCents', amount, 'filledCents', filled,
  'pendingAmendCents', pending, 'lastEventSeq', math.max(lastSeq, seq))
redis.call('HINCRBY', KEYS[3], 'version', 1)
if state ~= 'RESERVED' then redis.call('ZREM', KEYS[4], ARGV[6]) end
if TERMINAL[state] then
  redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))
else
  redis.call('PERSIST', KEYS[3])
end
return reply('APPLIED', alert)
