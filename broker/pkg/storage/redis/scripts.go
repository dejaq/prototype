package redis

// scripts hold scripts that need to be loaded into redis
var scripts = struct {
	insert          string
	getAndLease     string
	delete          string
	getByConsumerId string
}{
	insert: `
	    local bucket_key = KEYS[1]
		local message_key = KEYS[2]
	    local message_id = KEYS[3]
	    local end_lease_MS = KEYS[4]
	
	    -- check if messageId already exists and return error
		if redis.call("ZRANK", bucket_key, message_id) ~= false then
			return "1"
		end
		
		-- inset on timeline
		local ok = redis.call("ZADD", bucket_key, end_lease_MS, message_id)
		if ok ~= 1 then return "2" end
		
		-- inset on hashMap
		local okhmap = redis.call("HMSET", message_key, unpack(ARGV))
		
		-- check if is ok, rollback transaction if not
		if okhmap.ok ~= "OK" then  --TODO miniredis wants without .ok (not sure where is the issue)
	        local removeOk = redis.call("ZREM", bucket_key, message_id)
	        if removeOk ~= 1 then return "3" end
	        return "4"
		 end
	
		return "0"
	`,
	getAndLease: `
	    local timeline_key = KEYS[1]
	    local time_reference_MS = KEYS[2]
	    local lease_duration_MS = KEYS[3]
	    local limit = KEYS[4]
	    local consumer_id = KEYS[5]
	    local buckets_ids = ARGV
	
	    local data = {}
	
	    -- group ids and score
	    local function groupIdsScore(c)
	        local r = {}
	        local tmp = {}
	        for i, v in pairs(c) do
	            if i % 2 == 0 then
	                tmp.score = v
	                table.insert(r, tmp)
	                tmp = {}
	            else
	                tmp.id = v 
	            end
	        end
	        return r
	    end
	
	    -- iterate over buckets ids
	    for bidx, bucket_id in pairs(buckets_ids) do
	        local bucket_key = timeline_key .. "::" .. bucket_id
	
	        -- get message ids by bucket and iterate over them
	        local raw_ids_and_scores = redis.call("ZRANGEBYSCORE", bucket_key, "-inf", time_reference_MS, "LIMIT", "0", limit, "WITHSCORES")
	        local grouped_ids_and_score = groupIdsScore(raw_ids_and_scores)
	
	        for i, m in pairs(grouped_ids_and_score) do
	            -- return when reach limit
	            limit = limit - 1
				if limit < 0 then return data end
	
	            -- get message details
	            local message_key = bucket_key .. "::" .. m.id
	            local message = redis.call("HMGET", message_key, "ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version")
	
	            -- process only available messages (EndLeaseMS(score) <= now() || (EndLeaseMS(score) > now() && empty ConsumerId) 
	            if (m.score <= time_reference_MS or (m.score > time_reference_MS and message[12] == '')) then
	                -- calculate end_lease_message max(timeReference, score) + lease
	                local end_lease_MS = math.max(tonumber(time_reference_MS), tonumber(m.score)) + tonumber(lease_duration_MS)
	
	                local has_error = false 
	                local tmp = {}
	
	                -- update lease on timeline
					local ok = redis.call("ZADD", bucket_key, end_lease_MS, m.id)
					if ok ~= 0 then
	                    table.insert(tmp, "2")
	                    has_error = true
	                end
	
	                -- update consumerId and lease on message
					if has_error == false then 
						ok = redis.call("HMSET", message_key, "LockConsumerID", consumer_id)
						if ok.ok ~= "OK" then  --TODO miniredis wants without .ok (not sure where is the issue)
							-- Rollback if need, return 2 if success rollback
							ok = redis.call("ZADD", bucket_key, m.score, m.id)
	                        -- code 3: fail rollback
	                        -- code 2: success rollback
	                        local code = "2"
							if ok ~= 0 then code = "3" end
	                        table.insert(tmp, code)
	                        has_error = true
						end
					end
	
	                -- add messageId to consumerId sortedSet used in getByConsumerId
	                -- returns 1 => added, 0 => score updated (both can be considered success)
	                redis.call("ZADD",  timeline_key .. "::" .. consumer_id, end_lease_MS,  bucket_key .. "::" .. m.id)
	               
	                -- code 0: no errors
	                if has_error == false then table.insert(tmp, "0") end 
	                table.insert(tmp, tostring(end_lease_MS))
	                table.insert(tmp, message[1])
	                table.insert(tmp, message[2])
	                table.insert(tmp, message[3])
	                table.insert(tmp, message[4])
	                table.insert(tmp, message[5])
	                table.insert(tmp, consumer_id)
	                table.insert(tmp, message[7])
	                table.insert(tmp, message[8])
	                table.insert(data, tmp)
	            end
	        end
	    end
	
	    return data
	`,
	delete:          `return 100`,
	getByConsumerId: `
	    local consumer_key = KEYS[1]
	    local time_reference_MS = KEYS[2]
	
	    local data = {}
	
	    local message_ids = redis.call("ZRANGEBYSCORE", consumer_key, time_reference_MS, "+inf")
	    for idx, message_key in pairs(message_ids) do
	        local message = redis.call("HMGET", message_key, "ID", "TimestampMS", "BodyID", "Body", "ProducerGroupID", "LockConsumerID", "BucketID", "Version")
	        local tmp = {}
			table.insert(tmp, message[1])
			table.insert(tmp, message[2])
			table.insert(tmp, message[3])
			table.insert(tmp, message[4])
			table.insert(tmp, message[5])
			table.insert(tmp, message[6])
			table.insert(tmp, message[7])
			table.insert(tmp, message[8])
			table.insert(data, tmp)
	    end
	
	    return data
	`,
}
