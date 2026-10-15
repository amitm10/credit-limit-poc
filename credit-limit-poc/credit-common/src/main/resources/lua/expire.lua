-- Release an orphan reservation: approved, but never acknowledged by the venue.
-- Only RESERVED orders are eligible. Working orders (including GTC orders resting for
-- days) are never expired on a timer; they are released only by cancel/reject events.
--
-- KEYS[1] group limit  KEYS[2] account limit  KEYS[3] reservation  KEYS[4] pending set
-- ARGV[1] orderId  ARGV[2] cutoffMillis (reservations created before this are due)
-- ARGV[3] expiredTtlSeconds (kept long, so a very late ACK can still find and revive it)
--
-- Returns {status, detail, accountHeadroomCents, groupHeadroomCents, <reservation hash pairs>}
-- status: EXPIRED | NOT_DUE | SKIPPED | GONE

local s = redis.call('HMGET', KEYS[3], 'state', 'amountCents', 'filledCents', 'createdTs')
if not s[1] then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return {'GONE', '', '', ''}
end
if s[1] ~= 'RESERVED' then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return {'SKIPPED', s[1], '', ''}
end
if tonumber(s[4]) > tonumber(ARGV[2]) then
  return {'NOT_DUE', '', '', ''}
end

local remaining = math.max(tonumber(s[2]) - tonumber(s[3]), 0)
if remaining > 0 then
  redis.call('HINCRBY', KEYS[2], 'reserved', -remaining)
  redis.call('HINCRBY', KEYS[1], 'reserved', -remaining)
end
redis.call('HSET', KEYS[3], 'state', 'EXPIRED')
redis.call('HINCRBY', KEYS[3], 'version', 1)
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[3]))

local out = {'EXPIRED', '', '', ''}
local h = redis.call('HGETALL', KEYS[3])
for i = 1, #h do out[#out + 1] = h[i] end
return out
