local skynet = require "skynet"
local mysql = require "skynet.db.mysql"

local pool_size = 10    
local db_config = {     
	host = "127.0.0.1",
	port = 3306,
	database = "message_board",
	user = "root",
	password = "123456",
	max_packet_size = 1024 * 1024
}

local pool = {}
local CMD = {}

function CMD.start()
	for i = 1, pool_size do
		local db = mysql.connect(db_config)
		if db then
			table.insert(pool, db)
		else
			skynet.error("failed to connect to database")
		end
	end
end

function CMD.stop()
	for _, db in pairs(pool) do
		db:disconnect()
	end
	pool = {}
end

function CMD.execute(sql)
	local db = table.remove(pool)
	if not db then
		skynet.error("no available db connection")
		return
	end

	local res = db:query(sql)

	table.insert(pool, db)

	return res
end

skynet.start(function()
	skynet.dispatch("lua", function(_, _, command, ...)
		local f = CMD[command]
		skynet.ret(skynet.pack(f(...)))
	end)
end)
