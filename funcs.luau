local common = game:GetService("CoreGui").RobloxGui.Modules.Common
local commonutil = common.CommonUtil
local constants = commonutil:Clone()
constants.Name = "Constants"
constants.Parent = common
if not getgenv().file then
    getgenv().file = {}
end
if not getgenv().folder then
    getgenv().folder = {}
end
if not getgenv().folder then
    getgenv().folder = {}
end
local game = game
local Instance = Instance
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

do
    local _comm_channels = {}
    local _next_comm_channel_id = 1
    local _parallel_states = setmetatable({}, { __mode = "k" })

    local function _actor_clone(value, cache)
        local t = type(value)
        if t ~= "table" then
            return value
        end
        cache = cache or {}
        if cache[value] then
            return cache[value]
        end
        local out = {}
        cache[value] = out
        for k, v in pairs(value) do
            out[_actor_clone(k, cache)] = _actor_clone(v, cache)
        end
        return out
    end

    local function newChannelEvent()
        local connections = {}
        local ev = {}
        
        function ev:Connect(callback)
            assert(type(callback) == "function", "bad argument #1 to 'Connect' (function expected)", 2)
            local conn = {
                Connected = true,
                Callback = callback,
            }
            function conn.Disconnect()
                conn.Connected = false
            end
            connections[#connections + 1] = conn
            return conn
        end
        
        function ev:Once(callback)
            assert(type(callback) == "function", "bad argument #1 to 'Once' (function expected)", 2)
            local conn
            conn = self:Connect(function(...)
                if conn.Connected then
                    conn:Disconnect()
                    callback(...)
                end
            end)
            return conn
        end
        
        function ev:Wait()
            local thread = coroutine.running()
            local conn
            conn = self:Connect(function(...)
                if conn.Connected then
                    conn:Disconnect()
                    task.spawn(thread, ...)
                end
            end)
            return coroutine.yield()
        end
        
        function ev:Fire(...)
            local snapshot = {}
            for i = 1, #connections do
                snapshot[i] = connections[i]
            end
            for _, conn in ipairs(snapshot) do
                if conn.Connected then
                    task.spawn(conn.Callback, ...)
                end
            end
        end
        
        return ev
    end

    local function _new_comm_channel()
        local evt = newChannelEvent()
        local queue = {}
        local channel = {}
        channel.Event = evt
        
        function channel:Fire(...)
            local args = table.pack(...)
            local cloned = table.create(args.n)
            for i = 1, args.n do
                cloned[i] = _actor_clone(args[i])
            end
            queue[#queue + 1] = cloned[1]
            evt:Fire(table.unpack(cloned, 1, args.n))
        end
        
        function channel:send(...)
            self:Fire(...)
            return true
        end
        
        function channel:receive()
            if #queue == 0 then
                return nil
            end
            local first = queue[1]
            table.remove(queue, 1)
            return first
        end
        
        function channel:peek()
            return queue[1]
        end
        
        function channel:clear()
            table.clear(queue)
        end
        
        function channel:size()
            return #queue
        end
        
        function channel:destroy()
            if self._id then
                _comm_channels[self._id] = nil
            end
            table.clear(queue)
        end
        
        return channel
    end

    getgenv().getactors = function()
        local out = {}
        for _, v in ipairs(game:GetDescendants()) do
            if v.ClassName == "Actor" then
                out[#out + 1] = (type(ToProxy) == "function" and ToProxy(v)) or v
            end
        end
        return out
    end

    getgenv().create_comm_channel = function()
        local id = _next_comm_channel_id
        _next_comm_channel_id += 1
        local ch = _new_comm_channel()
        ch._id = id
        _comm_channels[id] = ch
        return id, ch
    end
    getgenv().createcommchannel = getgenv().create_comm_channel

    getgenv().get_comm_channel = function(id)
        if id == nil then
            return nil
        end
        return _comm_channels[tonumber(id)]
    end
    getgenv().getcommchannel = getgenv().get_comm_channel

    getgenv().isparallel = function(targetThread)
        if targetThread == nil then
            targetThread = coroutine.running()
            if type(targetThread) == "thread" and _parallel_states[targetThread] == true then
                return true
            end
            return rawget(getgenv(), "__actor_parallel__") == true
        end
        if type(targetThread) ~= "thread" then
            return false
        end
        return _parallel_states[targetThread] == true
    end

    if type(task) == "table" then
        local function setTaskMethod(methodName, func)
            local unlocked = false
            if type(setreadonly) == "function" then
                unlocked = pcall(setreadonly, task, false)
            end
            pcall(function()
                task[methodName] = func
            end)
            if unlocked and type(setreadonly) == "function" then
                pcall(setreadonly, task, true)
            end
        end

        local function hookTaskMethod(methodName)
            local nativeFunc = task[methodName]
            if type(nativeFunc) == "function" then
                setTaskMethod(methodName, function(...)
                    if rawget(getgenv(), "__actor_parallel__") == true then
                        return true
                    end
                    return nativeFunc(...)
                end)
            else
                setTaskMethod(methodName, function(...)
                    if rawget(getgenv(), "__actor_parallel__") == true then
                        return true
                    end
                    error(methodName .. "() may only be called from a script that is descendant of an Actor", 2)
                end)
            end
        end

        hookTaskMethod("desynchronize")
        hookTaskMethod("synchronize")
    end

    getgenv().run_on_actor = function(actor, fn_or_source, ...)
        assert(typeof(actor) == "Instance", "bad argument #1 to 'run_on_actor' (Instance expected)")
        assert(actor.ClassName == "Actor", "bad argument #1 to 'run_on_actor' (Actor expected)")
        local argc = select("#", ...)
        local args = table.pack(...)
        for i = 1, argc do
            args[i] = _actor_clone(args[i])
        end

        local function run_parallel(fn)
            local co = coroutine.create(function(...)
                local running = coroutine.running()
                _parallel_states[running] = true
                local g = getgenv()
                local prev = rawget(g, "__actor_parallel__")
                rawset(g, "__actor_parallel__", true)
                local ok, a, b, c, d, e = pcall(fn, ...)
                if prev == nil then
                    rawset(g, "__actor_parallel__", nil)
                else
                    rawset(g, "__actor_parallel__", prev)
                end
                _parallel_states[running] = nil
                if not ok then
                    error(a, 0)
                end
                return _actor_clone(a), _actor_clone(b), _actor_clone(c), _actor_clone(d), _actor_clone(e)
            end)

            local ok, a, b, c, d, e = coroutine.resume(co, table.unpack(args, 1, argc))
            _parallel_states[co] = nil
            if not ok then
                error(a, 2)
            end
            return a, b, c, d, e
        end

        if type(fn_or_source) == "function" then
            return run_parallel(fn_or_source)
        elseif type(fn_or_source) == "string" then
            local chunk, err = getgenv().loadstring(fn_or_source, "run_on_actor")
            if not chunk then
                error(err, 2)
            end
            return run_parallel(chunk)
        else
            error("bad argument #2 to 'run_on_actor' (function or string expected)", 2)
        end
    end
getgenv().run_on_actors = getgenv().run_on_actor
getgenv().runonactor = getgenv().run_on_actor
end
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

getgenv().Signal    = { new = makeSignal }
getgenv().makeSignal = makeSignal

local _saveinstance = nil
getgenv().saveinstance = function(options)
    options = options or {}
    assert(type(options) == "table", "invalid argument #1 to 'saveinstance' (table expected, got " .. type(options) .. ") ", 2)
    print("saveinstance Powered by UniversalSynSaveInstance | AGPL-3.0 license")
    _saveinstance = _saveinstance or getgenv().loadstring(getgenv().HttpGet("https://raw.githubusercontent.com/luau/SynSaveInstance/main/saveinstance.luau", true), "saveinstance")()
    return _saveinstance(options)
end
getgenv().savegame = getgenv().saveinstance
local _menv_cache = setmetatable({}, { __mode = "k" })

getgenv().getmenv = function()
    local currentThread = coroutine.running()
    if _menv_cache[currentThread] then
        return _menv_cache[currentThread]
    end
    local level = 2 
    local success = false
    local moduleEnv = nil
    while level < 10 do
        local info = debug.getinfo(level)
        if not info then break end
        local func = info.func
        if func and type(func) == "function" then
            local ok, env = pcall(function()
                return getfenv(func)
            end)
            
            if ok and env and type(env) == "table" then
                moduleEnv = env
                success = true
                break
            end
        end
        
        level = level + 1
    end 
    if success and moduleEnv then
        _menv_cache[currentThread] = moduleEnv
        return moduleEnv
    end  
    local fallbackEnv = _G
    _menv_cache[currentThread] = fallbackEnv
    return fallbackEnv
end
getgenv().getpropertychangedsignal = function(instance, property)
    assert(typeof(instance) == "Instance", "invalid argument #1 to 'getpropertychangedsignal' (Instance expected, got " .. typeof(instance) .. ")")
    assert(type(property) == "string",     "invalid argument #2 to 'getpropertychangedsignal' (string expected, got " .. type(property) .. ")")
    return instance:GetPropertyChangedSignal(property)
end
getgenv().getsimulationradius = function()
    local radius, maxRadius = 0, 0
    local plr = game:GetService("Players").LocalPlayer
    if plr then
        local ok1, v1 = pcall(function() return plr.SimulationRadius end)
        if ok1 and type(v1) == "number" then radius = v1 end
        local ok2, v2 = pcall(function() return plr.MaximumSimulationRadius end)
        if ok2 and type(v2) == "number" then
            maxRadius = v2
        else
            local gethiddenprop = gethiddenproperty or getgenv().gethiddenprop
            if type(gethiddenprop) == "function" then
                local ok3, v3 = pcall(gethiddenprop, plr, "MaximumSimulationRadius")
                if ok3 and type(v3) == "number" then maxRadius = v3 end
            end
        end
    end
    return radius, maxRadius
end

getgenv().setsimulationradius = function(radius, maxRadius)
    assert(type(radius) == "number", "invalid argument #1 to 'setsimulationradius' (number expected)")
    local plr = game:GetService("Players").LocalPlayer
    if plr then
        pcall(function() plr.SimulationRadius = radius end)
        if maxRadius then
            local ok = pcall(function() plr.MaximumSimulationRadius = maxRadius end)
            if not ok then
                local sethiddenprop = sethiddenproperty or getgenv().sethiddenprop
                if type(sethiddenprop) == "function" then pcall(sethiddenprop, plr, "MaximumSimulationRadius", maxRadius) end
            end
        end
    end
end
