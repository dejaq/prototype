package redis

// Scripts hold scripts that need to be loaded into redis
var Scripts = struct {
	insert      string
	getAndLease string
	delete      string
}{
	insert: `
	    local timeline_key = KEYS[1]
		local message_key = KEYS[2]
	    local message_id = KEYS[3]
	    local timestamp = KEYS[4]
	
	    -- check if messageId already exists and return error
		if redis.call("ZRANK", timeline_key, message_id) ~= false then
			return "1"
		end
		
		-- inset on timeline
		local ok = redis.call("ZADD", timeline_key, timestamp, message_id)
		if ok ~= 1 then
			return "2"
		end
		
		-- inset on hashMap
		local okhmap = redis.call("HMSET", message_key, unpack(ARGV))
		
		-- check if is ok, rollback transaction if not
		if okhmap ~= 'OK' then
	        local removeOk = redis.call("ZREM", timeline_key, message_id)
	        if removeOk ~= 1 then
		        return "3"
		    end
	      
	       return "4"
		 end
	
		-- return "0"
	`,
	getAndLease: `return 100`,
	delete:      `return 100`,
}
