local skynet = require "skynet"
local mysql = require "skynet.mysql"

-- 数据库连接池
local db_pool = {}

-- 初始化数据库连接池
function init_db_pool(max_conn, db_config)
    for i = 1, max_conn do
        local db, err = mysql.connect(db_config)
        if not db then
            print("Error connecting to database:", err)
            break
        end
        table.insert(db_pool, db)
    end
end

-- 从连接池中获取一个数据库连接
function get_db_connection()
    if #db_pool > 0 then
        return table.remove(db_pool, 1)
    else
        return nil
    end
end

-- 将数据库连接放回连接池
function release_db_connection(db)
    if db then
        table.insert(db_pool, db)
    end
end

-- 使用数据库连接执行查询语句
function execute_query(db, sql)
    if db then
        local res, err, errno, sqlstate = db:query(sql)
        if not res then
            print("Error executing query:", err, errno, sqlstate)
        else
            return res
        end
    end
end

-- 关闭数据库连接池中的所有连接
function close_db_pool()
    for i, db in ipairs(db_pool) do
        db:disconnect()
    end
    db_pool = {}
end

-- 示例用法
local max_conn = 10
local db_config = {
    host = "127.0.0.1",
    port = 3306,
    database = "message_board",
    user = "root",
    password = "123456",
}

init_db_pool(max_conn, db_config)

skynet.start(function()
    local db = get_db_connection()
    if db then
        local res = execute_query(db, "SELECT * FROM msgs")
        for i, row in ipairs(res) do
            print(i, row.id, row.text)
        end
        release_db_connection(db)
    end
end)

-- 在程序结束时关闭数据库连接池
skynet.register_onexit(function()
    close_db_pool()
end)
