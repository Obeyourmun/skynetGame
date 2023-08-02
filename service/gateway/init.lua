local skynet = require "skynet"
local socket = require "skynet.socket"
local s = require "service"
local runconfig = require "runconfig"
local netpack = require "skynet.netpack"

conns = {} --客户端fd连接
players = {} --玩家id

--连接类
function conn()
    local m = {
        fd = nil,
        playerid = nil,
    }
    return m
end

--玩家类
function gateplayer()
    local m = {
        playerid = nil,
        agent = nil,
		conn = nil,
    }
    return m
end

local str_pack = function(cmd, msg)
    return table.concat( msg, ",").."\r\n"
end

local str_unpack = function(msgstr)
    local msg = {}

    while true do
        local arg, rest = string.match( msgstr, "(.-),(.*)")
        if arg then
            msgstr = rest
            table.insert(msg, arg)
        else
            table.insert(msg, msgstr)
            break
        end
    end

    return msg[1], msg
end

--发消息给客户端
s.resp.send_by_fd = function(source, fd, msg)
    if not conns[fd] then
        return
    end

    local buff = str_pack(msg[1], msg)
    skynet.error("send "..fd.." ["..msg[1].."] {"..table.concat( msg, ",").."}")
	socket.write(fd, netpack.tostring(buff))
end

--发消息给代理服务agent
s.resp.send = function(source, playerid, msg)
	local gplayer = players[playerid]
    if gplayer == nil then
		return
    end
    local c = gplayer.conn
    if c == nil then
		return
    end
    
    s.resp.send_by_fd(nil, c.fd, msg)
end

--确认agent服务创建成功
s.resp.sure_agent = function(source, fd, playerid, agent)

	local conn = conns[fd]
	if not conn then --登陆过程中已经下线
		skynet.call("agentmgr", "lua", "reqkick", playerid, "未完成登陆即下线")
		return false
	end
	
	conn.playerid = playerid
	
    local gplayer = gateplayer()
    gplayer.playerid = playerid
    gplayer.agent = agent
	gplayer.conn = conn
    players[playerid] = gplayer
    
	return true
end

--处理客户端断开连接
local disconnect = function(fd)
    local c = conns[fd]
    if not c then
        return
    end

    local playerid = c.playerid
    --还没完成登录
    if not playerid then
        return
    --已在游戏中
    else
        players[playerid] = nil
        local reason = "断线"
        skynet.call("agentmgr", "lua", "reqkick", playerid, reason)
    end
end

--踢玩家下线
s.resp.kick = function(source, playerid)
    local gplayer = players[playerid]
    if not gplayer then
        return
    end

    local c = gplayer.conn
	players[playerid] = nil
	
    if not c then
        return
    end
    conns[c.fd] = nil

    disconnect(c.fd)
    socket.close(c.fd)
end

--处理收到的客户端信息
local process_msg = function(fd, msgstr)
    local cmd, msg = str_unpack(msgstr)
    skynet.error("recv "..fd.." ["..cmd.."] {"..table.concat( msg, ",").."}")

    local conn = conns[fd]
    local playerid = conn.playerid
    --尚未完成登录流程
    if not playerid then
        local node = skynet.getenv("node")
        local nodecfg = runconfig[node]
        local loginid = math.random(1, #nodecfg.login)
        local login = "login"..loginid
		skynet.send(login, "lua", "client", fd, cmd, msg)
    --完成登录流程
    else
        local gplayer = players[playerid]
        local agent = gplayer.agent
		skynet.send(agent, "lua", "client", cmd, msg)
    end
end

--处理缓冲区中的数据
local process_buff = function(fd, readbuff)
    while true do
        local msgstr, rest = string.match( readbuff, "(.-)\r\n(.*)")
        if msgstr then
            readbuff = rest
            process_msg(fd, msgstr)
        else
            return readbuff
        end
    end
end

--每一条连接接收数据处理
local recv_loop = function(fd)
    socket.start(fd)
    skynet.error("socket connected " ..fd)
    local readbuff = ""
    while true do
        local recvstr = socket.read(fd)
        if recvstr then
            readbuff = readbuff..recvstr
            local msg, sz = netpack.filter(readbuff)
            if msg then
                process_msg(fd, msg)
                readbuff = readbuff:sub(sz + 1)
            end
        else
            skynet.error("socket close " ..fd)
			disconnect(fd)
            socket.close(fd)
            return
        end
    end
end


--新连接处理
local connect = function(fd, addr)
    print("connect from " .. addr .. " " .. fd)
	local c = conn()
    conns[fd] = c
    c.fd = fd
    skynet.fork(recv_loop, fd)
end

function s.init()
    local node = skynet.getenv("node")
    local nodecfg = runconfig[node]
    local port = nodecfg.gateway[s.id].port

    local listenfd = socket.listen("0.0.0.0", port)
    skynet.error("Listen socket :", "0.0.0.0", port)
    socket.start(listenfd , connect)
end


s.start(...)