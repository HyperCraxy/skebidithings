local common = game:GetService("CoreGui").RobloxGui.Modules.Common
local commonutil = common.CommonUtil
local constants = commonutil:Clone()
constants.Name = "Constants"
constants.Parent = common
if not getgenv().folder then
    getgenv().folder = {}
end

local game = game
local Instance = Instance

local http = {}
http.request = request or http_request or syn_request

getgenv().http = http
getgenv().syn = getgenv().syn or {}
getgenv().syn.request = http.request

local bit32 = bit32 or bit or {}

if not bit32.band then
    bit32.band = function(a, b)
        local result = 0
        local bitval = 1
        while a > 0 and b > 0 do
            if a % 2 == 1 and b % 2 == 1 then
                result = result + bitval
            end
            bitval = bitval * 2
            a = math.floor(a / 2)
            b = math.floor(b / 2)
        end
        return result
    end
end

if not bit32.bor then
    bit32.bor = function(a, b)
        local result = 0
        local bitval = 1
        while a > 0 or b > 0 do
            if a % 2 == 1 or b % 2 == 1 then
                result = result + bitval
            end
            bitval = bitval * 2
            a = math.floor(a / 2)
            b = math.floor(b / 2)
        end
        return result
    end
end

if not bit32.bxor then
    bit32.bxor = function(a, b)
        local result = 0
        local bitval = 1
        while a > 0 or b > 0 do
            if (a % 2) ~= (b % 2) then
                result = result + bitval
            end
            bitval = bitval * 2
            a = math.floor(a / 2)
            b = math.floor(b / 2)
        end
        return result
    end
end

if not bit32.bnot then
    bit32.bnot = function(n)
        return (-1) - n
    end
end

if not bit32.lshift then
    bit32.lshift = function(a, b)
        return a * (2 ^ b)
    end
end

if not bit32.rshift then
    bit32.rshift = function(a, b)
        return math.floor(a / (2 ^ b))
    end
end

if not bit32.arshift then
    bit32.arshift = function(a, b)
        local result = math.floor(a / (2 ^ b))
        if a < 0 and b > 0 then
            result = result + (2 ^ (32 - b)) - 1
        end
        return result
    end
end

if not bit32.lrotate then
    bit32.lrotate = function(a, b)
        b = b % 32
        return bit32.bor(bit32.lshift(a, b), bit32.rshift(a, 32 - b))
    end
end

if not bit32.rrotate then
    bit32.rrotate = function(a, b)
        b = b % 32
        return bit32.bor(bit32.rshift(a, b), bit32.lshift(a, 32 - b))
    end
end

if not bit32.extract then
    bit32.extract = function(n, field, width)
        width = width or 1
        return bit32.band(bit32.rshift(n, field), (2 ^ width) - 1)
    end
end

if not bit32.replace then
    bit32.replace = function(n, v, field, width)
        width = width or 1
        local mask = (2 ^ width) - 1
        return bit32.bor(
            bit32.band(n, bit32.bnot(bit32.lshift(mask, field))),
            bit32.lshift(bit32.band(v, mask), field)
        )
    end
end

getgenv().bit32 = bit32
getgenv().bit = bit32

local _saveinstance = nil
getgenv().saveinstance = function(options)
    options = options or {}
    assert(type(options) == "table", "invalid argument #1 to 'saveinstance' (table expected, got " .. type(options) .. ") ", 2)
    print("saveinstance Powered by UniversalSynSaveInstance | AGPL-3.0 license")
    _saveinstance = _saveinstance or getgenv().loadstring(getgenv().HttpGet("https://raw.githubusercontent.com/luau/SynSaveInstance/main/saveinstance.luau", true), "saveinstance")()
    return _saveinstance(options)
end
getgenv().savegame = getgenv().saveinstance
local function makeSignal()
    local threads = {}
    local function reusableThreadCall(callback, thread, ...) callback(...); table.insert(threads, thread) end
    local function reusableThread() while true do reusableThreadCall(coroutine.yield()) end end
    local function disconnect(connection)
        if not connection.Connected then return end
        connection.Connected = nil
        local signal, previous, next = connection.Signal, connection.Previous, connection.Next
        if previous then previous.Next = next else signal.Tail = next end
        if next then next.Previous = previous else signal.Head = previous end
    end
    local Signal = {}
    Signal.__index = Signal
    Signal.Connect = function(signal, callback)
        assert(type(callback) == "function", "invalid argument #1 to 'Signal:Connect' (function expected, got " .. type(callback) .. ")")
        local head = signal.Head
        local connection = { Signal = signal, Previous = head, Callback = callback, Connected = true, Disconnect = disconnect }
        if head then head.Next = connection else signal.Tail = connection end
        signal.Head = connection
        return connection
    end
    Signal.Once = function(signal, callback)
        assert(type(callback) == "function", "invalid argument #1 to 'Signal:Once' (function expected, got " .. type(callback) .. ")")
        local head = signal.Head
        local connection
        connection = {
            Signal = signal, Previous = head,
            Callback = function(...)
                if not connection.Connected then return end
                connection.Connected = false
                local previous, next = connection.Previous, connection.Next
                if previous then previous.Next = next else signal.Tail = next end
                if next then next.Previous = previous else signal.Head = previous end
                callback(...)
            end,
            Connected = true, Disconnect = disconnect
        }
        if head then head.Next = connection else signal.Tail = connection end
        signal.Head = connection
        return connection
    end
    Signal.Wait = function(signal)
        local thread = coroutine.running()
        local head = signal.Head
        local connection
        connection = {
            Signal = signal, Previous = head, Connected = true, Disconnect = disconnect,
            Callback = function(...)
                if not connection.Connected then return end
                connection.Connected = false
                local previous, next = connection.Previous, connection.Next
                if previous then previous.Next = next else signal.Tail = next end
                if next then next.Previous = previous else signal.Head = previous end
                if coroutine.status(thread) == "suspended" then task.spawn(thread, ...) end
            end
        }
        if head then head.Next = connection else signal.Tail = connection end
        signal.Head = connection
        return coroutine.yield()
    end
    Signal.Fire = function(signal, ...)
        local connection = signal.Tail
        while connection do
            local nextConn = connection.Next
            if connection.Callback then
                local length = #threads
                if length == 0 then
                    local thread = coroutine.create(reusableThread)
                    coroutine.resume(thread)
                    task.spawn(thread, connection.Callback, thread, ...)
                else
                    local thread = threads[length]
                    threads[length] = nil
                    task.spawn(thread, connection.Callback, thread, ...)
                end
            end
            connection = nextConn
        end
    end
    Signal.DisconnectAll = function(signal)
        local connection = signal.Tail
        while connection do
            local nextConnection = connection.Next
            connection.Connected = nil; connection.Next = nil; connection.Previous = nil
            connection = nextConnection
        end
        signal.Tail = nil; signal.Head = nil
    end
    Signal.Destroy = function(signal)
        local connection = signal.Tail
        while connection do
            local nextConnection = connection.Next
            connection.Connected = nil; connection.Next = nil; connection.Previous = nil
            connection = nextConnection
        end
        signal.Tail = nil; signal.Head = nil
    end
    return setmetatable({}, Signal)
end

getgenv().Signal = { new = makeSignal }
getgenv().makeSignal = makeSignal
