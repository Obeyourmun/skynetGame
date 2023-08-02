local skynet = require "skynet"
local s = require "service"
local pb = require "pb"
pb.loadfile("../storage/player_data.proto")

s.client = {}
s.gate = nil
s.dbpool = skynet.uniqueservice("dbpool")

require "scene"

--处理客户端请求
s.resp.client = function(source, cmd, msg)
    s.gate = source
    if s.client[cmd] then
		local ret_msg = s.client[cmd]( msg, source)
		if ret_msg then
			skynet.send(source, "lua", "send", s.id, ret_msg)
		end
    else
        skynet.error("s.resp.client fail", cmd)
    end
end

--打工测试
s.client.work = function(msg)
	s.data.coin = s.data.coin + 1
	return {"work", s.data.coin}
end

s.resp.kick = function(source)
    s.leave_scene()

    -- 获取数据库连接
    local db = skynet.call(s.dbpool, "lua", "get")
    if not db then
        skynet.error("failed to get db connection")
        return
    end

    local data_serialized = pb.encode("playerdata.BaseInfo", s.data)

    -- 存储玩家数据
    local res = skynet.call(db, "lua", "execute", string.format("UPDATE player_data SET base_info = '%s' WHERE playerid = %d", data_serialized, s.id))
    if not res then
        skynet.error("failed to save player data to db")
        return
    end

    -- 释放数据库连接
    skynet.call(s.dbpool, "lua", "put", db)

end

--退出服务
s.resp.exit = function(source)
	skynet.exit()
end

--给网关发送信息
s.resp.send = function(source, msg)
	skynet.send(s.gate, "lua", "send", s.id, msg)
end

--开启游戏时加载角色数据
s.init = function()
    -- 获取数据库连接
    local db = skynet.call(s.dbpool, "lua", "get")
    if not db then
        skynet.error("failed to get db connection")
        return
    end

    -- 查询玩家数据
    local res = skynet.call(db, "lua", "execute", string.format("SELECT base_info FROM player_data WHERE playerid = %d", s.id))
    --如果找不到玩家数据则新建
    if not res or #res < 1 then   
        skynet.error("player data not found in db, creating new player data")
        -- 默认数据
        local default_data = {
            playerid = s.id,
            coin = 100,
            name = "New Player",
            level = 1,
            last_login_time = os.time(),
            sword = 1,
        }
        
        -- 序列化默认
        local default_data_serialized = pb.encode("playerdata.BaseInfo", default_data)

        -- 存入数据库
        skynet.call(db, "lua", "execute", string.format("INSERT INTO player_data (playerid, base_info) VALUES (%d, '%s')", s.id, default_data_serialized))

        -- 设置玩家数据为默认数据
        s.data = default_data
    else
        -- 反序列化玩家数据
        local data = pb.decode("playerdata.BaseInfo", res[1].base_info)

        -- 保存玩家数据
        s.data = {
            coin = data.coin,
            playerid = data.playerid,
            name = data.name,
            level = data.level,
            last_login_time = data.last_login_time,
            sword = data.sword,
        }
    end

    -- 释放数据库连接
    skynet.call(s.dbpool, "lua", "put", db)
end



s.start(...)