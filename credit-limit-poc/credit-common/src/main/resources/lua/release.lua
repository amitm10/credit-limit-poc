-- Compensation for the dual write. Called when the reservation was taken in Redis but the
-- RESERVED event could not be published to Kafka, so the API is about to answer 503.
-- The reservation is released and deleted, so a gateway retry makes a fresh check.
-- Only a reservation that nothing has touched since the check (version 1) can be undone.
--
-- KEYS[1] group limit  KEYS[2] account limit  KEYS[3] reservation  KEYS[4] pending set
-- ARGV[1] orderId  ARGV[2] requestHash
--
-- Returns {status, detail, '', '', <final reservation hash pairs, before deletion>}
-- status: RELEASED | SKIPPED | NOT_FOUND

local s = redis.call('HMGET', KEYS[3], 'state', 'amountCents', 'filledCents', 'requestHash', 'version')
if not s[1] or s[4] ~= ARGV[2] then return {'NOT_FOUND', '', '', ''} end
if s[1] ~= 'RESERVED' or tonumber(s[5]) ~= 1 then return {'SKIPPED', s[1], '', ''} end

local remaining = math.max(tonumber(s[2]) - tonumber(s[3]), 0)
if remaining > 0 then
  redis.call('HINCRBY', KEYS[2], 'reserved', -remaining)
  redis.call('HINCRBY', KEYS[1], 'reserved', -remaining)
end
redis.call('HSET', KEYS[3], 'state', 'CANCELLED')
redis.call('HINCRBY', KEYS[3], 'version', 1)

local out = {'RELEASED', '', '', ''}
local h = redis.call('HGETALL', KEYS[3])
for i = 1, #h do out[#out + 1] = h[i] end
redis.call('DEL', KEYS[3])
redis.call('ZREM', KEYS[4], ARGV[1])
return out
