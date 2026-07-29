print("Ryxzve was here")
local clonerefs = setmetatable({}, { __mode = "kv" })
local weakrefs = setmetatable({}, { __mode = "v" })
local proxyToWeakKey = setmetatable({}, { __mode = "k" })
local invalidatedProxies = setmetatable({}, { __mode = "k" })

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
    getgenv().runonactor = getgenv().run_on_actor
end

local function base64enc(data)
    local b = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
    local out = {}
    local len = #data
    local i = 1
    while i <= len do
        local a = data:byte(i)
        local bb = (i + 1 <= len) and data:byte(i + 1) or 0
        local c = (i + 2 <= len) and data:byte(i + 2) or 0
        local n = a * 65536 + bb * 256 + c
        local i1 = math.floor(n / 262144) % 64
        local i2 = math.floor(n / 4096) % 64
        local i3 = math.floor(n / 64) % 64
        local i4 = n % 64
        out[#out + 1] = b:sub(i1 + 1, i1 + 1)
        out[#out + 1] = b:sub(i2 + 1, i2 + 1)
        if i + 1 <= len then
            out[#out + 1] = b:sub(i3 + 1, i3 + 1)
        else
            out[#out + 1] = '='
        end
        if i + 2 <= len then
            out[#out + 1] = b:sub(i4 + 1, i4 + 1)
        else
            out[#out + 1] = '='
        end
        i = i + 3
    end
    return table.concat(out)
end

local sha256_core = (function()
    local K = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
    }

    local band, bor, bxor, bnot, rshift, lshift =
        bit32.band, bit32.bor, bit32.bxor, bit32.bnot, bit32.rshift, bit32.lshift

    local function rrotate(x, n)
        return bor(rshift(x, n), lshift(x, 32 - n))
    end

    local function str2blks(msg)
        local len = #msg
        local bit_len = len * 8
        local pad_len = (55 - (len % 64)) % 64
        local len_bytes = ""
        for i = 1, 8 do
            len_bytes = string.char(bit_len % 256) .. len_bytes
            bit_len = math.floor(bit_len / 256)
        end
        local msg_with_pad = msg .. string.char(0x80) .. string.rep(string.char(0), pad_len) .. len_bytes
        
        local blks = {}
        for i = 1, #msg_with_pad, 64 do
            local blk = {}
            for j = 0, 15 do
                local w = 0
                for k = 0, 3 do
                    w = w * 256 + msg_with_pad:byte(i + j*4 + k)
                end
                blk[j+1] = w
            end
            table.insert(blks, blk)
        end
        return blks
    end

    return function(msg)
        local h1, h2, h3, h4, h5, h6, h7, h8 = 
            0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
            0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19

        local blks = str2blks(msg)

        for _, blk in ipairs(blks) do
            local w = {}
            for i = 1, 16 do w[i] = blk[i] end
            for i = 17, 64 do
                local s0 = bxor(bxor(rrotate(w[i-15], 7), rrotate(w[i-15], 18)), rshift(w[i-15], 3))
                local s1 = bxor(bxor(rrotate(w[i-2], 17), rrotate(w[i-2], 19)), rshift(w[i-2], 10))
                w[i] = (w[i-16] + s0 + w[i-7] + s1) % 2^32
            end

            local a, b, c, d, e, f, g, h = h1, h2, h3, h4, h5, h6, h7, h8

            for i = 1, 64 do
                local S1 = bxor(bxor(rrotate(e, 6), rrotate(e, 11)), rrotate(e, 25))
                local ch = bxor(band(e, f), band(bnot(e), g))
                local temp1 = (h + S1 + ch + K[i] + w[i]) % 2^32
                local S0 = bxor(bxor(rrotate(a, 2), rrotate(a, 13)), rrotate(a, 22))
                local maj = bxor(bxor(band(a, b), band(a, c)), band(b, c))
                local temp2 = (S0 + maj) % 2^32

                h = g; g = f; f = e
                e = (d + temp1) % 2^32
                d = c; c = b; b = a
                a = (temp1 + temp2) % 2^32
            end

            h1 = (h1 + a) % 2^32
            h2 = (h2 + b) % 2^32
            h3 = (h3 + c) % 2^32
            h4 = (h4 + d) % 2^32
            h5 = (h5 + e) % 2^32
            h6 = (h6 + f) % 2^32
            h7 = (h7 + g) % 2^32
            h8 = (h8 + h) % 2^32
        end

        local hash = ""
        local vals = {h1, h2, h3, h4, h5, h6, h7, h8}
        for _, v in ipairs(vals) do
            for i = 24, 0, -8 do
                hash = hash .. string.char(band(rshift(v, i), 0xFF))
            end
        end
        return hash
    end
end)()

local g = getgenv()

local _executorClosures = setmetatable({}, { __mode = "k" })
local function _markExecutor(f)
    if type(f) == "function" then _executorClosures[f] = true end
    return f
end

local readonlyTables = setmetatable({}, { __mode = "k" })
local _native_setreadonly = setreadonly
local _native_isreadonly = isreadonly

getgenv().setreadonly = function(t, ro)
    assert(type(t) == "table", "invalid argument #1 to 'setreadonly' (table expected, got " .. type(t) .. ")")
    if type(_native_setreadonly) == "function" then
        pcall(_native_setreadonly, t, ro)
    elseif ro and type(table.freeze) == "function" then
        pcall(table.freeze, t)
    end
    if ro then
        readonlyTables[t] = true
    else
        readonlyTables[t] = nil
    end
end

getgenv().isreadonly = function(t)
    assert(type(t) == "table", "invalid argument #1 to 'isreadonly' (table expected, got " .. type(t) .. ")")
    if readonlyTables[t] then return true end
    if type(table.isfrozen) == "function" then
        local ok, res = pcall(table.isfrozen, t)
        if ok and res then return true end
    end
    if type(_native_isreadonly) == "function" then
        local ok, res = pcall(_native_isreadonly, t)
        if ok then return res end
    end
    return false
end

getgenv().setrawmetatable = function(obj, mt)
    assert(type(mt) == "table", "invalid argument #2 to 'setrawmetatable' (table expected)")
    if type(obj) == "table" then
        local ok = pcall(setmetatable, obj, mt)
        if ok then return obj end
        if type(debug.setmetatable) == "function" then
            local ok2 = pcall(debug.setmetatable, obj, mt)
            if ok2 then return obj end
        end
        error("setrawmetatable failed, table metatable is locked", 2)
    end
    return obj
end

local nativeCrypt = type(crypt) == "table" and crypt or {}
local customCrypt = {}

for k, v in pairs(nativeCrypt) do
    customCrypt[k] = v
end

customCrypt.random = function(n)
    if type(n) ~= "number" then error("bad argument #1 to 'random' (number expected)", 2) end
    if n < 0 then error("bad argument #1 to 'random' (must be >= 0)", 2) end
    local res = {}
    for i = 1, n do
        res[i] = string.char(math.random(0, 255))
    end
    return table.concat(res)
end

customCrypt.hmac = function(key, data, algo)
    if algo ~= "sha256" then error("Unsupported algorithm", 2) end
    local block_size = 64
    if #key > block_size then key = sha256_core(key) end
    key = key .. string.rep("\0", block_size - #key)

    local o_key_pad = {}
    local i_key_pad = {}
    for i = 1, #key do
        o_key_pad[i] = string.char(bit32.bxor(key:byte(i), 0x5c))
        i_key_pad[i] = string.char(bit32.bxor(key:byte(i), 0x36))
    end
    o_key_pad = table.concat(o_key_pad)
    i_key_pad = table.concat(i_key_pad)
    
    local inner_hash = sha256_core(i_key_pad .. data)
    local final_hash = sha256_core(o_key_pad .. inner_hash)
    return base64enc(final_hash)
end

local function setGlob(name, val)
    local ok = pcall(function() g[name] = val end)
    if not ok then pcall(rawset, g, name, val) end
end

setGlob("crypt", customCrypt)
getgenv().crypt = customCrypt

local _trackedScripts = setmetatable({}, { __mode = "k" })
local _signalOwners = setmetatable({}, { __mode = "k" })
local _origInstanceNew = Instance.new
local function _hookedInstanceNew(className, parent)
    local inst
    if parent ~= nil then
        inst = _origInstanceNew(className, parent)
    else
        inst = _origInstanceNew(className)
    end
    pcall(function()
        if typeof(inst) == "Instance" then
            local cn = inst.ClassName
            if cn == "LocalScript" or cn == "ModuleScript" or cn == "Script" then
                _trackedScripts[inst] = true
            end
            if cn == "BindableEvent" then
                _signalOwners[inst.Event] = inst
            end
        end
    end)
    return inst
end

local function applyInstanceHook()
    Instance.new = _hookedInstanceNew
end
local okInstHook = pcall(applyInstanceHook)
if not okInstHook and type(setreadonly) == "function" then
    pcall(setreadonly, Instance, false)
    pcall(applyInstanceHook)
    pcall(setreadonly, Instance, true)
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

getgenv().Signal = { new = makeSignal }
getgenv().makeSignal = makeSignal

getgenv().getloadedmodules = function(ExcludeCore)
    local loaded = {}

    if ExcludeCore == true then
        for _, Descendant in pairs(game:GetDescendants()) do
            if Descendant:IsA("ModuleScript") or Descendant:IsA("CoreScript") then
                table.insert(loaded, Descendant)
            end
        end
    else
        for _, Descendant in pairs(game:GetDescendants()) do
            if Descendant:IsA("ModuleScript") then
                table.insert(loaded, Descendant)
            end
        end
    end

    assert(#loaded ~= 0 and loaded[1] ~= nil, "No modules available", 2)

    return loaded
end

getgenv().getscripts = function()
    local t = {}
    local seen = {}
    local coreGui = game:GetService("CoreGui")
    local corePackages = game:GetService("CorePackages")

    local function isUnderCore(instance)
        local parent = instance.Parent
        while parent do
            if parent == coreGui or parent == corePackages then
                return true
            end
            parent = parent.Parent
        end
        return false
    end

    local function shouldInclude(v)
        if typeof(v) ~= "Instance" then return false end
        if v:IsA("LocalScript") or v:IsA("ModuleScript") then
            return not isUnderCore(v)
        elseif v:IsA("Script") then
            local ok, runContext = pcall(function() return v.RunContext end)
            if ok and runContext == Enum.RunContext.Client then
                return not isUnderCore(v)
            end
            return false
        end
        return false
    end

    for inst in pairs(_trackedScripts) do
        if typeof(inst) == "Instance" then
            if inst.Parent == nil then
                if shouldInclude(inst) and not seen[inst] then
                    table.insert(t, inst)
                    seen[inst] = true
                end
            end
        else
            _trackedScripts[inst] = nil
        end
    end

    for _, v in ipairs(game:GetDescendants()) do
        if not seen[v] and shouldInclude(v) then
            table.insert(t, v)
            seen[v] = true
        end
    end

    return t
end

getgenv().cansignalreplicate = function(signal)
    assert(typeof(signal) == "RBXScriptSignal", "invalid argument #1 to 'cansignalreplicate' (RBXScriptSignal expected, got " .. typeof(signal) .. ")")
    local replicatingClasses = { RemoteEvent=true, RemoteFunction=true, UnreliableRemoteEvent=true }
    local replicatingSignals = { "OnClientEvent", "OnServerEvent", "OnClientInvoke", "OnServerInvoke" }
    for _, svcName in ipairs({ "ReplicatedStorage", "Workspace", "Players", "ReplicatedFirst" }) do
        local ok2, svc = pcall(function() return game:GetService(svcName) end)
        if ok2 then
            for _, desc in ipairs(svc:GetDescendants()) do
                if replicatingClasses[desc.ClassName] then
                    for _, sigName in ipairs(replicatingSignals) do
                        local ok3, sig = pcall(function() return desc[sigName] end)
                        if ok3 and sig == signal then return true end
                    end
                end
            end
        end
    end
    return false
end

local _signalArgTypes = {
    Touched={"BasePart"}, TouchEnded={"BasePart"}, Changed={"string"}, ChildAdded={"Instance"},
    ChildRemoved={"Instance"}, DescendantAdded={"Instance"}, DescendantRemoving={"Instance"},
    AncestryChanged={"Instance","Instance"}, AttributeChanged={"string"}, ChildrenChanged={"Instance"},
    OnClientEvent={"any"}, OnServerEvent={"Player","any"}, PropertyChanged={"string"},
    Heartbeat={"number"}, Stepped={"number","number"}, RenderStepped={"number"},
    PostSimulation={"number"}, PreSimulation={"number"}, PreRender={"number"}, PreAnimation={"number"},
    MembershipChanged={"Player"}, PlayerAdded={"Player"}, PlayerRemoving={"Player"},
    CharacterAdded={"Model"}, CharacterRemoving={"Model"}, Fired={}, Event={},
}

getgenv().getsignalarguments = function(signal)
    assert(typeof(signal) == "RBXScriptSignal", "invalid argument #1 to 'getsignalarguments' (RBXScriptSignal expected, got " .. typeof(signal) .. ")")
    local sigStr  = tostring(signal)
    local sigName = sigStr:match("Signal%s+(.+)") or sigStr:match("%.(%w+)$") or sigStr
    if _signalArgTypes[sigName] then
        local result = _signalArgTypes[sigName]
        return #result == 0 and { "any" } or result
    end
    local args, conn = nil, nil
    pcall(function()
        conn = signal:Once(function(...)
            args = {}
            for i = 1, select("#", ...) do table.insert(args, typeof(select(i, ...))) end
        end)
    end)
    if conn then pcall(function() conn:Disconnect() end) end
    if args and #args > 0 then return args end
    return { "any" }
end

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

local function MarkerFunction()
end
local getptr = getpointer or getaddress or function(v) return nil end
local markerAddr = getptr(MarkerFunction)
local envAddr = getptr(getfenv())
if markerAddr and envAddr and type(Bridge) == "function" then
    pcall(function()
        Bridge("register_trusted", { 
            marker = string.format("%X", markerAddr), 
            env = string.format("%X", envAddr) 
        })
    end)
end

local hookedMetamethods = {}
local savedMetatables = {}
local _hookDepth = setmetatable({}, { __mode = "k" })

getgenv().checkcaller = function()
    local thread = coroutine.running()
    local key = thread or "__main__"
    return (_hookDepth[key] or 0) == 0
end

getgenv().hookmetamethod = function(object, metamethodName, hook)
    assert(type(object) == "table" or type(object) == "userdata" or typeof(object) == "Instance", "invalid argument #1")
    assert(type(metamethodName) == "string", "invalid argument #2")
    assert(type(hook) == "function", "invalid argument #3")
    local targetMt = savedMetatables[object]

    if not targetMt then
        local success, mt = pcall(getmetatable, object)
        if success and type(mt) == "table" then
            targetMt = mt
        elseif getgenv().getrawmetatable then
            targetMt = getgenv().getrawmetatable(object)
        else
            targetMt = {}
            pcall(setmetatable, object, targetMt)
        end
    end

    if type(targetMt) ~= "table" then
        error("cannot get metatable for object", 2)
    end

    savedMetatables[object] = targetMt

    local original = rawget(targetMt, metamethodName) or function(...) end

    local wrappedHook
    if metamethodName == "__namecall" then
        wrappedHook = function(self, ...)
            local thread = coroutine.running()
            local key = thread or "__main__"
            _hookDepth[key] = (_hookDepth[key] or 0) + 1
            local results = table.pack(hook(self, ...))
            _hookDepth[key] = (_hookDepth[key] or 1) - 1
            if _hookDepth[key] <= 0 then _hookDepth[key] = nil end
            return table.unpack(results, 1, results.n)
        end
    else
        wrappedHook = function(...)
            local thread = coroutine.running()
            local key = thread or "__main__"
            _hookDepth[key] = (_hookDepth[key] or 0) + 1
            local results = table.pack(hook(...))
            _hookDepth[key] = (_hookDepth[key] or 1) - 1
            if _hookDepth[key] <= 0 then _hookDepth[key] = nil end
            return table.unpack(results, 1, results.n)
        end
    end

    if not hookedMetamethods[targetMt] then
        hookedMetamethods[targetMt] = {}
    end
    hookedMetamethods[targetMt][metamethodName] = original

    local wasReadOnly = false
    if type(isreadonly) == "function" then
        local ok, ro = pcall(isreadonly, targetMt)
        wasReadOnly = ok and ro
    end

    if type(setreadonly) == "function" then
        pcall(setreadonly, targetMt, false)
    end

    local success, err = pcall(rawset, targetMt, metamethodName, wrappedHook)

    if wasReadOnly and type(setreadonly) == "function" then
        pcall(setreadonly, targetMt, true)
    end

    if not success then
        error("Failed to hook metamethod: " .. tostring(err), 2)
    end

    return original
end

local function GenerateError(object)
    local _, err = xpcall(function()
        object:__namecall()
    end, function()
        return debug.info(2, "f")
    end)
    return err
end
local FirstTest = GenerateError(OverlapParams.new())
local SecondTest = GenerateError(Color3.new())
local cachedmethods = setmetatable({}, { __mode = "k" })
local function extractMethod(err)
    if type(err) == "string" then
        return err:match("^(.+) is not a valid member of %w+$")
    end
end
local function resolveMethod()
    local _, e1 = pcall(FirstTest)
    local method = extractMethod(e1)
    if not method then
        local _, e2 = pcall(SecondTest)
        method = extractMethod(e2)
    end
    return method
end
getgenv().getnamecallmethod = function()
    local thread = coroutine.running()

    local cached = rawget(cachedmethods, thread)
    if cached then
        rawset(cachedmethods, thread, nil)
        return cached
    end
    local method = resolveMethod()
    if not method or method == "__namecall" then
        local fixerdata = newproxy(true)
        local fixermeta = getmetatable(fixerdata)
        fixermeta.__namecall = function()
            local fallback = resolveMethod()
            if fallback and fallback ~= "__namecall" then
                rawset(cachedmethods, thread, fallback)
            end
        end
        fixerdata:__namecall()
        return rawget(cachedmethods, thread)
    end
    rawset(cachedmethods, thread, method)
    rawset(cachedmethods, thread, nil)
    return method
end
getgenv().setnamecallmethod = function(method)
    assert(type(method) == "string", "invalid argument #1 to 'setnamecallmethod' (string expected, got " .. type(method) .. ")")
    local thread = coroutine.running()
    rawset(cachedmethods, thread, method)
end

if getgenv().replicatesignal == nil then
    getgenv().replicatesignal = function(signal, ...)
        assert(typeof(signal) == "RBXScriptSignal", "invalid argument #1 to 'replicatesignal' (RBXScriptSignal expected, got " .. typeof(signal) .. ")")
        local args = table.pack(...)   
        local isReplicable = false
        local replicableSignals = {
            "Triggered", "Activated", "Deactivated", "MouseClick", "MouseEnter", "MouseLeave",
            "Changed", "ChildAdded", "ChildRemoved", "DescendantAdded", "DescendantRemoved",
        }    
        local getConnectionsFn = type(getconnections) == "function" and getconnections or getgenv().getconnections
        if type(getConnectionsFn) == "function" then
            local ok, connections = pcall(getConnectionsFn, signal)
            if ok and type(connections) == "table" and #connections > 0 then
                for _, conn in ipairs(connections) do
                    if type(conn) == "table" then
                        for _, sigName in ipairs(replicableSignals) do
                            if string.find(tostring(conn), sigName) then
                                isReplicable = true
                                break
                            end
                        end
                    end
                    if isReplicable then break end
                end
            end
        end       
        if not isReplicable then
            error("Cannot replicate this signal: signal is not replicable or does not support replication", 2)
        end       
        local success = false
        pcall(function()
            local remoteEvent = Instance.new("RemoteEvent")
            remoteEvent.Name = "REPLICATE_SIGNAL"
            remoteEvent.Parent = game:GetService("ReplicatedStorage")
            
            task.spawn(function()
                remoteEvent:FireServer(signal, table.unpack(args, 1, args.n))
            end)
            
            task.wait(0.1)
            remoteEvent:Destroy()
            success = true
        end)
        
        if success then
            return true
        end
        
        error("Failed to replicate signal to server", 2)
    end
end

getgenv().firetouchinterest = function(toTouch, touchWith, on)
    assert(typeof(toTouch)  == "Instance", "invalid argument #1 to 'firetouchinterest' (Instance expected, got " .. typeof(toTouch) .. ")")
    assert(typeof(touchWith) == "Instance", "invalid argument #2 to 'firetouchinterest' (Instance expected, got " .. typeof(touchWith) .. ")")
    if on == 0 then return end
    if toTouch.ClassName == "TouchTransmitter" then toTouch = toTouch:FindFirstAncestorWhichIsA("BasePart") or toTouch end
    local cf, anc = toTouch.CFrame, toTouch.CanCollide
    toTouch.CanCollide = false; toTouch.CFrame = touchWith.CFrame
    task.wait()
    toTouch.CFrame = cf; toTouch.CanCollide = anc
end
getgenv().firetouchtransmitter = getgenv().firetouchinterest

getgenv().fireproximityprompt = function(p)
    assert(typeof(p) == "Instance" and p:IsA("ProximityPrompt"), "invalid argument #1 to 'fireproximityprompt' (ProximityPrompt expected)")
    local Hold, Distance, Enabled, Thing = p.HoldDuration, p.MaxActivationDistance, p.Enabled, p.RequiresLineOfSight
    p.MaxActivationDistance = math.huge; p.HoldDuration = 0; p.Enabled = true; p.RequiresLineOfSight = false
    local function getParent()
        for _, cls in ipairs({ "BasePart", "Part", "MeshPart" }) do
            local parent = p:FindFirstAncestorOfClass(cls); if parent then return parent end
        end
    end
    local parentPart = getParent()
    local tempPart = nil
    if not parentPart then
        tempPart = Instance.new("Part", workspace)
        tempPart.Transparency = 1; tempPart.CanCollide = false; tempPart.Anchored = true
        p.Parent = tempPart; parentPart = tempPart
    end
    local originalCFrame = parentPart.CFrame
    local player = game:GetService("Players").LocalPlayer
    local hrp    = player and player.Character and player.Character:FindFirstChild("HumanoidRootPart")
    local function getTargetCFrame()
        local h = player.Character and player.Character:FindFirstChild("Head")
        if h then return h.CFrame * CFrame.new(0, 0, -2)
        elseif hrp then return hrp.CFrame * CFrame.new(0, 1.5, -2) end
        return originalCFrame
    end
    local locked = true
    local lockConn = game:GetService("RunService").Heartbeat:Connect(function()
        if locked then parentPart.CFrame = getTargetCFrame() end
    end)
    parentPart.CFrame = getTargetCFrame(); task.wait(0.05)
    p:InputHoldBegin(); task.wait(0.05); p:InputHoldEnd()
    locked = false; lockConn:Disconnect()
    p.HoldDuration = Hold; p.MaxActivationDistance = Distance; p.Enabled = Enabled; p.RequiresLineOfSight = Thing
    parentPart.CFrame = originalCFrame
    if tempPart then task.delay(1, function() if tempPart and tempPart.Parent then tempPart:Destroy() end end) end
end

local _binds = setmetatable({}, { __mode = "k" })
local _remoteCallbacks = setmetatable({}, { __mode = "k" })

local fInstance = {}
fInstance.new = function(name, par)
    assert(type(name) == "string", "invalid argument #1 to 'Instance.new' (string expected, got " .. type(name) .. ")")
    if name == "BindableFunction" then
        local ins   = _origInstanceNew(name, par)
        local proxy = newproxy(true)
        local mt    = getmetatable(proxy)
        mt.__index = function(_, k)
            if k == "OnInvoke" then return _binds[proxy] end
            local v = ins[k]
            if type(v) == "function" then return function(s, ...) return v(ins, ...) end end
            return v
        end
        mt.__newindex = function(_, k, v)
            if k == "OnInvoke" or k == "onInvoke" then _binds[proxy] = v
            else ins[k] = v end
        end
        mt.__tostring  = function() return tostring(ins) end
        mt.__metatable = getmetatable(ins)
        return proxy
    elseif name == "RemoteFunction" then
        local ins   = _origInstanceNew(name, par)
        local proxy = newproxy(true)
        local mt    = getmetatable(proxy)
        mt.__index = function(_, k)
            if k == "OnServerInvoke" then return _remoteCallbacks[proxy] end
            local v = ins[k]
            if type(v) == "function" then return function(s, ...) return v(ins, ...) end end
            return v
        end
        mt.__newindex = function(_, k, v)
            if k == "OnServerInvoke" or k == "onServerInvoke" then 
                _remoteCallbacks[proxy] = v
                pcall(function() ins[k] = v end)
            else ins[k] = v end
        end
        mt.__tostring  = function() return tostring(ins) end
        mt.__metatable = getmetatable(ins)
        return proxy
    end
    return _hookedInstanceNew(name, par)
end
if type(Instance.fromExisting) == "function" then
    fInstance.fromExisting = function(obj) return Instance.fromExisting(obj) end
else
    fInstance.fromExisting = function(obj) return obj end
end
getgenv().Instance = fInstance

getgenv().getcallbackvalue = function(bind)
    assert(bind ~= nil, "invalid argument #1 to 'getcallbackvalue' (value expected, got nil)")
    local callback = _binds[bind]
    if callback then
        return callback
    end
    callback = _remoteCallbacks[bind]
    if callback then
        return callback
    end
    return nil
end
getgenv().getcallbackmember = getgenv().getcallbackvalue

getgenv().getsignalargumentsinfo = function(signal)
    assert(typeof(signal) == "RBXScriptSignal", "invalid argument #1 to 'getsignalargumentsinfo' (RBXScriptSignal expected, got " .. typeof(signal) .. ")")
    
    local signalStr = tostring(signal)
    local signalName = signalStr:match("Signal%s+(.+)") or signalStr:match("%.(%w+)$") or signalStr
    
    local argumentTypes = {
        Touched = { { Type = "BasePart", Name = "hit" } },
        TouchEnded = { { Type = "BasePart", Name = "hit" } },
        Changed = { { Type = "string", Name = "property" } },
        ChildAdded = { { Type = "Instance", Name = "child" } },
        ChildRemoved = { { Type = "Instance", Name = "child" } },
        DescendantAdded = { { Type = "Instance", Name = "descendant" } },
        DescendantRemoving = { { Type = "Instance", Name = "descendant" } },
        AncestryChanged = { { Type = "Instance", Name = "child" }, { Type = "Instance", Name = "parent" } },
        AttributeChanged = { { Type = "string", Name = "attribute" } },
        ChildrenChanged = { { Type = "Instance", Name = "child" } },
        OnClientEvent = { { Type = "any", Name = "..." } },
        OnServerEvent = { { Type = "Player", Name = "player" }, { Type = "any", Name = "..." } },
        PropertyChanged = { { Type = "string", Name = "property" } },
        Heartbeat = { { Type = "number", Name = "deltaTime" } },
        Stepped = { { Type = "number", Name = "time" }, { Type = "number", Name = "deltaTime" } },
        RenderStepped = { { Type = "number", Name = "deltaTime" } },
        PostSimulation = { { Type = "number", Name = "deltaTime" } },
        PreSimulation = { { Type = "number", Name = "deltaTime" } },
        PreRender = { { Type = "number", Name = "deltaTime" } },
        PreAnimation = { { Type = "number", Name = "deltaTime" } },
        MembershipChanged = { { Type = "Player", Name = "player" } },
        PlayerAdded = { { Type = "Player", Name = "player" } },
        PlayerRemoving = { { Type = "Player", Name = "player" } },
        CharacterAdded = { { Type = "Model", Name = "character" } },
        CharacterRemoving = { { Type = "Model", Name = "character" } },
        Fired = {},
        Event = {},
    }
    
    if argumentTypes[signalName] then
        return argumentTypes[signalName]
    end
    
    local args, conn = nil, nil
    pcall(function()
        conn = signal:Once(function(...)
            args = {}
            for i = 1, select("#", ...) do
                local val = select(i, ...)
                local t = typeof(val)
                if t == "Instance" then
                    t = val.ClassName
                end
                table.insert(args, { Type = t, Name = "arg" .. i })
            end
        end)
    end)
    
    if conn then pcall(function() conn:Disconnect() end) end
    
    if args and #args > 0 then return args end
    
    return { { Type = "any", Name = "..." } }
end

getgenv().getsignalwhitelist = function()
    return {
        { Event = "Touched", Parent = "BasePart" },
        { Event = "TouchEnded", Parent = "BasePart" },
        { Event = "Changed", Parent = "Instance" },
        { Event = "ChildAdded", Parent = "Instance" },
        { Event = "ChildRemoved", Parent = "Instance" },
        { Event = "DescendantAdded", Parent = "Instance" },
        { Event = "DescendantRemoving", Parent = "Instance" },
        { Event = "AncestryChanged", Parent = "Instance" },
        { Event = "AttributeChanged", Parent = "Instance" },
        { Event = "ChildrenChanged", Parent = "Instance" },
        { Event = "OnClientEvent", Parent = "RemoteEvent" },
        { Event = "OnServerEvent", Parent = "RemoteEvent" },
        { Event = "OnClientInvoke", Parent = "RemoteFunction" },
        { Event = "OnServerInvoke", Parent = "RemoteFunction" },
        { Event = "PropertyChanged", Parent = "ValueBase" },
        { Event = "Heartbeat", Parent = "RunService" },
        { Event = "Stepped", Parent = "RunService" },
        { Event = "RenderStepped", Parent = "RunService" },
        { Event = "PostSimulation", Parent = "RunService" },
        { Event = "PreSimulation", Parent = "RunService" },
        { Event = "PreRender", Parent = "RunService" },
        { Event = "PreAnimation", Parent = "RunService" },
        { Event = "MembershipChanged", Parent = "Players" },
        { Event = "PlayerAdded", Parent = "Players" },
        { Event = "PlayerRemoving", Parent = "Players" },
        { Event = "CharacterAdded", Parent = "Player" },
        { Event = "CharacterRemoving", Parent = "Player" },
        { Event = "Fired", Parent = "BindableEvent" },
        { Event = "Event", Parent = "BindableEvent" },
    }
end

getgenv().getgc = function(includeTables)
    includeTables = includeTables or false
    local results, seen = {}, {}
    local function add(v)
        if v == nil or seen[v] then return end
        seen[v] = true; table.insert(results, v)
    end
    if type(debug) == "table" and type(debug.getregistry) == "function" then
        local ok, registry = pcall(debug.getregistry)
        if ok and type(registry) == "table" then
            for _, v in pairs(registry) do
                local t = type(v)
                if t == "function" or t == "userdata" or (includeTables and t == "table") then add(v) end
            end
        end
    end
    local function scrapeTable(t, depth)
        if depth > 4 or not t or seen[t] then return end
        seen[t] = true
        if includeTables then add(t) end
        for k, v in pairs(t) do
            local tv = type(v)
            if tv == "function" then add(v)
            elseif tv == "userdata" then add(v)
            elseif tv == "table" and depth < 4 then scrapeTable(v, depth + 1) end
            local tk = type(k)
            if tk == "function" then add(k)
            elseif tk == "table" and depth < 4 then scrapeTable(k, depth + 1) end
        end
    end
    scrapeTable(getfenv(0), 0); scrapeTable(_G, 0)
    if type(shared) == "table" then scrapeTable(shared, 0) end
    local ok, descendants = pcall(function() return game:GetDescendants() end)
    if ok and type(descendants) == "table" then for _, v in ipairs(descendants) do if type(v) == "userdata" then add(v) end end end
    return results
end

getgenv().filtergc = function(typeName, options)
    assert(type(typeName) == "string", "invalid argument #1 to 'filtergc' (string expected, got " .. type(typeName) .. ")")
    options = type(options) == "table" and options or {}
    local predicate  = type(options.filter)     == "function" and options.filter     or nil
    local methodName = type(options.methodName) == "string"   and options.methodName or nil
    local reg, seen, results = getgenv().getreg(), {}, {}
    for _, v in pairs(reg) do
        local t = type(v)
        if t == typeName and not seen[v] then
            seen[v] = true
            local keep = true
            if methodName then local ok, has = pcall(function() return type(v[methodName]) == "function" end); if not (ok and has) then keep = false end end
            if keep and predicate then local ok, pass = pcall(predicate, v); if not (ok and pass) then keep = false end end
            if keep then table.insert(results, v) end
        end
    end
    return results
end

getgenv().getsenv = function(script_instance)
    assert(typeof(script_instance) == "Instance", "invalid argument #1 to 'getsenv' (Instance expected, got " .. typeof(script_instance) .. ")")
    if script_instance:IsA("LuaSourceContainer") and not script_instance.Enabled then
        error("Script is not running", 2)
    end
    local env = getgenv()
    return setmetatable({ script = script_instance }, {
        __index    = function(self, k) return rawget(self, k) or env[k] end,
        __newindex = function(self, k, v) if not pcall(function() env[k] = v end) then rawset(self, k, v) end end,
    })
end

getgenv().getcallingscript = function()
    for lvl = 3, 0, -1 do
        local f = debug.info(lvl, "f"); if not f then continue end
        local ok, s = pcall(function() return rawget(getfenv(f), "script") end)
        if ok and s and typeof(s) == "Instance" and s:IsA("BaseScript") then return s end
    end
    return nil
end

getgenv().getconnections = function(signal)
    assert(typeof(signal) == "RBXScriptSignal", "invalid argument #1 to 'getconnections' (RBXScriptSignal expected, got " .. typeof(signal) .. ")", 2)
    local connections = {}
    if type(getconnections) == "function" then
        local ok, result = pcall(getconnections, signal)
        if ok and type(result) == "table" then
            for _, nativeConn in ipairs(result) do
                if type(nativeConn) == "table" then
                    local proxy = setmetatable({
                        Enabled = nativeConn.Enabled ~= false,
                        ForeignState = false,
                        LuaConnection = true,
                        Function = nativeConn.Function,
                        Thread = nativeConn.Thread or coroutine.create(function() end),
                    }, {
                        __index = nativeConn,
                    })
                    function proxy:Fire(...)
                        if self.Enabled and self.Function then
                            task.spawn(self.Function, ...)
                        end
                    end
                    function proxy:Defer(...)
                        if self.Enabled and self.Function then
                            task.defer(self.Function, ...)
                        end
                    end
                    function proxy:Disable()
                        self.Enabled = false
                    end
                    function proxy:Enable()
                        self.Enabled = true
                    end
                    function proxy:Disconnect()
                        self.Enabled = false
                        if nativeConn.Disconnect then
                            nativeConn:Disconnect()
                        end
                    end
                    table.insert(connections, proxy)
                end
            end
            return connections
        end
    end
    local testCallback = function() end
    local testConnection = signal:Connect(testCallback)
    if testConnection then
        local proxy = {
            Enabled = true,
            ForeignState = false,
            LuaConnection = true,
            Function = testCallback,
            Thread = coroutine.create(function() end),
        }
        function proxy:Fire(...)
            if self.Enabled and self.Function then
                task.spawn(self.Function, ...)
            end
        end
        function proxy:Defer(...)
            if self.Enabled and self.Function then
                task.defer(self.Function, ...)
            end
        end
        function proxy:Disable()
            self.Enabled = false
        end
        function proxy:Enable()
            self.Enabled = true
        end
        function proxy:Disconnect()
            self.Enabled = false
            testConnection:Disconnect()
        end
        table.insert(connections, proxy)
    end
    return connections
end
getgenv().getsignals = getgenv().getconnections

local _closureNames = setmetatable({}, { __mode = "k" })
local nativeDebug = type(debug) == "table" and debug or {}
local customDebug = {}

for k, v in pairs(nativeDebug) do
    customDebug[k] = v
end

local _native_debug_info = debug.info
customDebug.info = function(f, what)
    if type(f) == "function" and type(what) == "string" and what:find("n") and _closureNames[f] then
        local results = {}
        for i = 1, #what do
            local c = what:sub(i, i)
            if c == "n" then
                results[i] = _closureNames[f]
            else
                local ok, res = pcall(_native_debug_info, f, c)
                results[i] = ok and res or nil
            end
        end
        return table.unpack(results, 1, #what)
    end
    return _native_debug_info(f, what)
end

customDebug.traceback = customDebug.traceback or function(msg, lvl) return msg or "" end
customDebug.profilebegin = customDebug.profilebegin or function() end
customDebug.profileend = customDebug.profileend or function() end

customDebug.getupvalue = function(func, num)
    local founded
    pcall(function() setfenv(func, { print = function(funcc) founded = funcc end }); func() end)
    return founded
end

customDebug.getupvalues = function(func)
    local founded
    pcall(function() setfenv(func, { print = function(funcc) founded = funcc end }); func() end)
    return { founded }
end
customDebug.setupvalue = customDebug.setupvalue or (type(g.setupvalue) == "function" and g.setupvalue) or function(f, i, v) return nil end

customDebug.getconstants = function(func)
    if type(getgenv().getconstants) == "function" then
        local ok, res = pcall(getgenv().getconstants, func)
        if ok then return res end
    end
    local info = debug.getinfo(func, "s")
    if info.what == "C" then
        error("C closures are not supported")
    end
    return {}
end
customDebug.getconstant = customDebug.getconstant or (type(g.getconstant) == "function" and g.getconstant) or function(f, i) return nil end
customDebug.setconstant = customDebug.setconstant or (type(g.setconstant) == "function" and g.setconstant) or function(f, i, v) return nil end

customDebug.getprotos = customDebug.getprotos or (type(g.getprotos) == "function" and g.getprotos) or function(f) return {} end
customDebug.getproto = customDebug.getproto or (type(g.getproto) == "function" and g.getproto) or function(f, i) return nil end

customDebug.getstack = customDebug.getstack or (type(g.getstack) == "function" and g.getstack) or function(...) return nil end
customDebug.setstack = customDebug.setstack or (type(g.setstack) == "function" and g.setstack) or function(...) return nil end

customDebug.getregistry = customDebug.getregistry or (type(g.getregistry) == "function" and g.getregistry) or function() return {} end

setGlob("debug", customDebug)
setGlob("getupvalues", customDebug.getupvalues)
setGlob("getupvalue", customDebug.getupvalue)
setGlob("setupvalue", customDebug.setupvalue)
setGlob("setupval", customDebug.setupvalue)
setGlob("getconstants", customDebug.getconstants)
setGlob("getconstant", customDebug.getconstant)
setGlob("setconstant", customDebug.setconstant)
setGlob("setvalue", customDebug.setconstant)
setGlob("getprotos", customDebug.getprotos)
setGlob("getproto", customDebug.getproto)
setGlob("getstack", customDebug.getstack)
setGlob("setstack", customDebug.setstack)
setGlob("getregistry", customDebug.getregistry)
setGlob("getinfo", customDebug.getinfo)

local hookedFunctions = {}
local oldWrappers = {}

local function doRestore(fn)
    local entry = hookedFunctions[fn]
    if not entry then
        return nil
    end

    local restored = entry.oldWrapper or entry.original
    oldWrappers[entry.oldWrapper] = nil

    if entry.previous then
        hookedFunctions[fn] = entry.previous
    else
        hookedFunctions[fn] = nil
    end

    return restored
end

local function hookfunction(original, hook)
    assert(type(original) == "function", "arg #1 must be a function")
    assert(type(hook) == "function", "arg #2 must be a function")

    local isRestoringWithWrapper = oldWrappers[hook]
    if isRestoringWithWrapper then
        local targetOriginal = isRestoringWithWrapper
        local entry = hookedFunctions[targetOriginal]
        if entry and entry.oldWrapper == hook then
            doRestore(targetOriginal)
            return hook
        end
    end

    local previousEntry = hookedFunctions[original]

    local oldWrapper = function(...)
        local currentEntry = hookedFunctions[original]
        hookedFunctions[original] = nil

        local results = table.pack(original(...))

        hookedFunctions[original] = currentEntry

        return table.unpack(results, 1, results.n)
    end

    if type(newcclosure) == "function" then
        oldWrapper = newcclosure(oldWrapper)
    end

    local entry = {
        type = "LuaFunction",
        original = original,
        hook = hook,
        oldWrapper = oldWrapper,
        previous = previousEntry,
    }

    hookedFunctions[original] = entry
    oldWrappers[oldWrapper] = original

    return oldWrapper
end

local function restorefunction(fn)
    assert(type(fn) == "function", "arg #1 must be a function")
    local entry = hookedFunctions[fn]
    if not entry then error("restorefunction: function is not hooked", 2) end
    return doRestore(fn)
end

local function ishookedfunction(fn)
    assert(type(fn) == "function", "invalid argument #1 to 'ishookedfunction' (function expected, got " .. type(fn) .. ")")
    return hookedFunctions[fn] ~= nil
end

local function getoriginalfunction(fn)
    assert(type(fn) == "function", "invalid argument #1 to 'getoriginalfunction' (function expected, got " .. type(fn) .. ")")
    local entry = hookedFunctions[fn]
    if entry then
        return entry.oldWrapper or entry.original
    end
    return fn
end

getgenv().hookfunction = hookfunction
getgenv().hookfunc = hookfunction
getgenv().replaceclosure = hookfunction
getgenv().restorefunction = restorefunction
getgenv().restorefunc = restorefunction
getgenv().ishookedfunction = ishookedfunction
getgenv().isfunctionhooked = ishookedfunction
getgenv().getoriginalfunction = getoriginalfunction

local _native_clonefunction = clonefunction
getgenv().clonefunction = function(f)
    assert(type(f) == "function", "invalid argument #1 to 'clonefunction' (function expected, got " .. type(f) .. ")")

    local dumpOk, dumped = pcall(string.dump, f)
    if dumpOk and type(dumped) == "string" and #dumped > 0 then
        local name = debug.info(f, "n") or ""
        local env = getfenv(f)
        local loadOk, cloned = pcall(load, dumped, name, "bt", env)
        if loadOk and type(cloned) == "function" then
            local getupvals = getgenv().getupvalues
            local setupval = getgenv().setupvalue
            if type(getupvals) == "function" and type(setupval) == "function" then
                local ok2, upvalues = pcall(getupvals, f)
                if ok2 and type(upvalues) == "table" then
                    for i, uv in ipairs(upvalues) do
                        pcall(setupval, cloned, i, uv)
                    end
                end
            end
            return cloned
        end
    end

    if type(_native_clonefunction) == "function" then
        local ok, result = pcall(_native_clonefunction, f)
        if ok and type(result) == "function" then
            return result
        end
    end

    return function(...)
        return f(...)
    end
end

local _native_iscclosure = iscclosure
local _native_newcclosure = newcclosure
getgenv().newcclosure = function(func, name)
    assert(type(func) == "function", "invalid argument #1 to 'newcclosure' (function expected, got " .. type(func) .. ")")
    local result
    if type(_native_iscclosure) == "function" and _native_iscclosure(func) then
        result = func
    elseif type(_native_newcclosure) == "function" then
        local ok, res = pcall(_native_newcclosure, func, name)
        if ok and type(res) == "function" then
            result = res
        else
            local ok2, res2 = pcall(_native_newcclosure, func)
            if ok2 and type(res2) == "function" then
                result = res2
            end
        end
    end
    if not result then
        result = coroutine.wrap(function(...) while true do coroutine.yield(func(...)) end end)
    end
    _markExecutor(result)
    if name then
        _closureNames[result] = name
    end
    return result
end

getgenv().makereadonly = function(t)
    if type(setreadonly) == "function" then
        pcall(setreadonly, t, true)
    end
end

getgenv().makewriteable = function(t)
    assert(type(t) == "table", "invalid argument #1 to 'makewriteable' (table expected, got " .. type(t) .. ")")
    if not table.isfrozen(t) then return t end
    return setmetatable({}, { __index = t, __newindex = function(_, k, v) rawset(t, k, v) end, __len = function() return #t end, __tostring = function() return tostring(t) end })
end

getgenv().getfunctionhash = function(f)
    if type(f) ~= "function" then 
        error("bad argument #1 to 'getfunctionhash' (function expected)", 2) 
    end
    if type(iscclosure) == "function" and iscclosure(f) then
        error("lua function expected", 2)
    end

    local numParams, isVararg
    pcall(function()
        numParams, isVararg = debug.info(f, "a")
    end)
    numParams = tonumber(numParams) or 0
    isVararg = not not isVararg

    local fingerprint = {}
    fingerprint[1] = tostring(numParams)
    fingerprint[2] = isVararg and "1" or "0"

    if numParams == 0 then
        local callOk, r1, r2, r3, r4, r5 = pcall(f)
        if callOk then
            local resultFingerprints = {}
            for _, v in ipairs({r1, r2, r3, r4, r5}) do
                local vt = type(v)
                if vt == "string" then
                    resultFingerprints[#resultFingerprints + 1] = "s" .. #v .. ":" .. v
                elseif vt == "number" then
                    resultFingerprints[#resultFingerprints + 1] = "n:" .. tostring(v)
                elseif vt == "boolean" then
                    resultFingerprints[#resultFingerprints + 1] = "b:" .. tostring(v)
                elseif vt == "table" then
                    resultFingerprints[#resultFingerprints + 1] = "t:" .. tostring(#v)
                end
            end
            fingerprint[3] = table.concat(resultFingerprints, ",")
        end
    end

    local hashStr = table.concat(fingerprint, "|")
    local state = sha256_core(hashStr)
    local extra = sha256_core(state .. hashStr)
    local bytes = state .. extra:sub(1, 16)
    local hex = ""
    for i = 1, #bytes do
        hex = hex .. string.format("%02x", string.byte(bytes, i))
    end
    return hex
end

do
    getgenv().firesignal = function(signal, ...)
        assert(typeof(signal) == "RBXScriptSignal", "invalid argument #1 to 'firesignal' (RBXScriptSignal expected, got " .. tostring(typeof(signal)) .. ")")

        local args = table.pack(...)

        local ownerInst = _signalOwners[signal]
        local messageId = ownerInst and ownerInst.Name or "MYRIAD_FIRESIGNAL"

        local getConnsFn = type(getconnections) == "function" and getconnections or getgenv().getconnections
        if type(getConnsFn) == "function" then
            local ok, conns = pcall(getConnsFn, signal)
            if ok and type(conns) == "table" then
                for _, conn in ipairs(conns) do
                    if type(conn) == "table" and conn.Enabled ~= false and type(conn.Function) == "function" then
                        local fn = conn.Function
                        task.spawn(function()
                            fn(table.unpack(args, 1, args.n))
                        end)
                    end
                end
            end
        end

        local actorsFn = getgenv().getactors
        if type(actorsFn) == "function" then
            local ok, actors = pcall(actorsFn)
            if ok and type(actors) == "table" then
                for _, actor in ipairs(actors) do
                    local a = actor
                    if typeof(a) == "Instance" and a.ClassName == "Actor" then
                        pcall(function()
                            a:SendMessage(messageId, table.unpack(args, 1, args.n))
                        end)
                    end
                end
            end
        end

        return true
    end
end

local _native_typeof = typeof
getgenv().typeof = function(v)
    if type(v) == "userdata" then
        local ref = clonerefs[v]
        if ref then return _native_typeof(ref) end
    end
    return _native_typeof(v)
end

do
    getgenv().cloneref = function(obj)
        assert(typeof(obj) == "Instance", "invalid argument #1 to 'cloneref' (Instance expected, got " .. typeof(obj) .. ")")
        for proxy, ref in pairs(clonerefs) do
            if ref == obj then
                return proxy
            end
        end
        local weakKey = {}
        weakrefs[weakKey] = obj
        local proxy = newproxy(true)
        local meta = getmetatable(proxy)
        meta.__index = function(t, n)
            local ref = weakrefs[weakKey]
            if not ref then
                error("Instance has been garbage collected")
            end
            local v = ref[n]
            if typeof(v) == "function" then
                return function(self, ...)
                    if self == t then self = ref end
                    return v(self, ...)
                end
            end
            return v
        end
        meta.__newindex = function(t, n, v)
            local ref = weakrefs[weakKey]
            if not ref then
                error("Instance has been garbage collected")
            end
            ref[n] = v
        end
        meta.__tostring = function(t)
            local ref = weakrefs[weakKey]
            if not ref then
                return "Instance (garbage collected)"
            end
            return tostring(ref)
        end
        meta.__eq = function(a, b)
            local ra = clonerefs[a] or a
            local rb = clonerefs[b] or b
            if invalidatedProxies[a] or invalidatedProxies[b] then
                return rawequal(a, b)
            end
            return ra == rb
        end
        meta.__metatable = getmetatable(obj)
        proxyToWeakKey[proxy] = weakKey
        clonerefs[proxy] = obj
        task.defer(function()
            local startTime = tick()
            while tick() - startTime < 10 do
                for i = 1, 50 do
                    task.wait()
                end
            end
        end)

        return proxy
    end
    getgenv().clonereference = getgenv().cloneref

    getgenv().compareinstances = function(a, b)
        assert(type(a) == "userdata", "invalid argument #1 to 'compareinstances' (userdata expected, got " .. type(a) .. ")")
        assert(type(b) == "userdata", "invalid argument #2 to 'compareinstances' (userdata expected, got " .. type(b) .. ")")
        if invalidatedProxies[a] or invalidatedProxies[b] then
            return rawequal(a, b)
        end
        return (clonerefs[a] or a) == (clonerefs[b] or b)
    end

    local _originalSetmetatable = setmetatable

    getgenv().setmetatable = function(t, mt)
        if type(t) == "table" and type(mt) == "table" and mt.__mode == "v" then
            local hasTable = false
            local hasInstance = false
            local tableIndex = nil
            local instanceIndex = nil
            local instance = nil
            for i, v in pairs(t) do
                if type(v) == "table" then
                    hasTable = true
                    tableIndex = i
                elseif typeof(v) == "Instance" then
                    hasInstance = true
                    instanceIndex = i
                    instance = v
                end
            end
            if hasTable and hasInstance and tableIndex then
                local result = _originalSetmetatable(t, mt)
                task.delay(0.01, function()
                    if instance then
                        pcall(function()
                            instance.Parent = nil
                        end)
                        pcall(function()
                            instance:Destroy()
                        end)
                        local proxiesToClear = {}
                        for proxy, ref in pairs(clonerefs) do
                            if ref == instance then
                                table.insert(proxiesToClear, proxy)
                            end
                        end
                        
                        for _, proxy in ipairs(proxiesToClear) do
                            local wk = proxyToWeakKey[proxy]
                            if wk then
                                weakrefs[wk] = nil
                            end
                            proxyToWeakKey[proxy] = nil
                            clonerefs[proxy] = nil
                        end
                    end
                    t[instanceIndex] = nil
                    task.wait(0.01)
                    t[tableIndex] = nil
                end)
                
                return result
            end
        end
        return _originalSetmetatable(t, mt)
    end

    local origGetService = game.GetService
    local function hookedGetService(self, serviceName)
        local real = origGetService(self, serviceName)
        local infoOk, infoType = pcall(debug.info, 2, "what")
        if infoOk and infoType == "Lua" then
            return getgenv().cloneref(real)
        end
        return real
    end
    
    local gameMt = getrawmetatable and getrawmetatable(game) or getmetatable(game)
    if gameMt then
        if type(setreadonly) == "function" then pcall(setreadonly, gameMt, false) end
        local origIndex = gameMt.__index
        gameMt.__index = function(t, k)
            if k == "GetService" then return hookedGetService end
            if type(origIndex) == "function" then return origIndex(t, k) end
            return origIndex[k]
        end
        if type(setreadonly) == "function" then pcall(setreadonly, gameMt, true) end
    end
end

do
    local _isExecutorClosure = function(func)
        if _executorClosures[func] then return true end
        
        local infoOk, src = pcall(debug.info, func, "s")
        if infoOk and src ~= "[C]" then
            local fenvOk, fenv = pcall(getfenv, func)
            if fenvOk and type(fenv) == "table" then
                local env = getfenv(0)
                if rawequal(fenv, env) then return true end
                local mtOk, mt = pcall(getmetatable, fenv)
                if mtOk and type(mt) == "table" then
                    local ok, idx = pcall(rawget, mt, "__index")
                    if ok and rawequal(idx, env) then return true end
                end
            end
        end
        return false
    end

    getgenv().isourclosure = function(func)
        assert(type(func) == "function", "invalid argument #1 to 'isourclosure' (function expected, got " .. type(func) .. ")")
        local ok, result = pcall(_isExecutorClosure, func)
        return ok and result or false
    end
    getgenv().isexecutorclosure = getgenv().isourclosure
    getgenv().checkclosure = getgenv().isourclosure

    for k, v in pairs(getgenv()) do
        if type(v) == "function" then
            _executorClosures[v] = true
        end
    end
end

do
    local cache = {cached = {}}
    local cacheproxied = {}
    local cacheobjects = {}
    local cacheclonerefs = {}
    local cacheinvalidated = {}

    local proxied = cacheproxied
    local objects = cacheobjects
    local rtypeof = typeof

    if not rawget(_G, "_serviceProxyCache") then
        rawset(_G, "_serviceProxyCache", {})
    end
    local _serviceProxyCache = rawget(_G, "_serviceProxyCache")

    local function isModifiableParent(instance)
        if instance:IsA("ServiceProvider") or instance:FindFirstAncestorOfClass("ServiceProvider") then
            return false
        end
        local parent = instance
        while parent do
            if parent == game:GetService("CoreGui") or parent == game:GetService("CorePackages") then
                return false
            end
            parent = parent.Parent
        end
        return true
    end

    function cache.iscached(t)
        if cache.cached[t] == 'r' then return false end
        local data = cacheproxied[t]
        if data then return not data.dead end
        if rtypeof(t) == "Instance" and cacheobjects[t] then return true end
        return true
    end

    function cache.invalidate(t)
        local realT = t
        if type(t) == "userdata" and clonerefs[t] then
            invalidatedProxies[t] = true
            realT = clonerefs[t]
        end

        cache.cached[t] = 'r'
        local data = proxied[t]
        local rawObj
        if data then
            rawObj = data.object
            data.dead = true
            objects[rawObj] = nil
            for name, proxy in pairs(_serviceProxyCache) do
                if proxy == t then
                    _serviceProxyCache[name] = nil
                    break
                end
            end
        elseif rtypeof(realT) == "Instance" then
            rawObj = realT
            local d = objects[rawObj]
            if d then
                d.dead = true
                objects[rawObj] = nil
            end
            for name, proxy in pairs(_serviceProxyCache) do
                if proxy == t or proxy == rawObj then
                    _serviceProxyCache[name] = nil
                    break
                end
            end
        end
        pcall(function()
            if rawObj and isModifiableParent(rawObj) then
                rawObj.Parent = nil
            end
        end)
    end

    function cache.validate(obj)
        local realObj = obj
        if cacheproxied[obj] then
            realObj = cacheproxied[obj].object
        end
        cacheinvalidated[realObj] = nil
        if not cacheobjects[realObj] then
            cacheproxyobject(realObj)
        else
            local data = cacheobjects[realObj]
            if data then
                data.dead = false
            end
        end
    end

    function cache.replace(x, y)
        local xdata = cacheproxied[x]
        local ydata = cacheproxied[y]

        local yobj
        if ydata and not ydata.dead then
            yobj = ydata.object
        elseif cacheclonerefs[y] then
            yobj = cacheclonerefs[y]
        elseif typeof(y) == "Instance" then
            yobj = y
        end

        local xobj
        if xdata and not xdata.dead then
            xobj = xdata.object
        elseif cacheclonerefs[x] then
            xobj = cacheclonerefs[x]
        elseif typeof(x) == "Instance" then
            xobj = x
        end

        if not xobj or not yobj then
            return false
        end

        if xdata then
            if xdata.object then
                cacheobjects[xdata.object] = nil
            end
            xdata.object = yobj
            cacheobjects[yobj] = xdata
        elseif typeof(x) == "Instance" and not cacheproxied[x] then
            local newdata = {
                object = yobj,
                proxy = x,
                meta = getmetatable(x),
                namecalls = {},
                dead = false
            }
            cacheproxied[x] = newdata
            cacheobjects[yobj] = newdata
        end

        if ydata and ydata.object then
            cacheobjects[ydata.object] = ydata
        end

        cache.cached[x] = nil

        pcall(function()
            if yobj and isModifiableParent(yobj) and xobj and isModifiableParent(xobj) then
                yobj.Parent = xobj.Parent
                yobj.Name = xobj.Name
                xobj.Parent = nil
            end
        end)

        return true
    end

    function cacheproxyobject(obj, namecalls)
        if cacheinvalidated[obj] then
            return obj 
        end
        
        if cacheobjects[obj] then
            local data = cacheobjects[obj]
            if data.dead then
                return obj 
            end
            return data.proxy
        end
        
        namecalls = namecalls or {}
        local proxy = newproxy(true)
        local meta = getmetatable(proxy)
        meta.__metatable = getmetatable(obj)

        local data = {}
        data.object = obj
        data.proxy = proxy
        data.meta = meta
        data.namecalls = namecalls
        data.dead = false

        cacheproxied[proxy] = data
        cacheobjects[obj] = data
        return proxy
    end

    getgenv().cacheproxyobject = cacheproxyobject
    getgenv().cache = cache
end

do
    local _renvCache = nil
    getgenv().getrenv = function()
        if _renvCache then return _renvCache end
        local t = {}
        local baseEnv = getfenv(0)
        for k, v in pairs(baseEnv) do
            rawset(t, k, v)
        end
        rawset(t, "table", table)
        rawset(t, "typeof", typeof)
        rawset(t, "game", game)
        rawset(t, "Game", game)
        rawset(t, "script", script)
        rawset(t, "workspace", workspace)
        rawset(t, "Workspace", workspace)
        rawset(t, "getmetatable", getmetatable)
        rawset(t, "setmetatable", setmetatable)
        rawset(t, "require", require)
        rawset(t, "print", print)
        rawset(t, "_G", t)
        rawset(t, "_ENV", t)
        _renvCache = t
        return t
    end
end

getgenv().openfilesdialog = function() return nil end
getgenv().openfolderdialog = function() return nil end
getgenv().savefiledialog = function() return nil end
getgenv().openfiledialog = function() return nil end

getgenv().getsynasset = getcustomasset
getgenv().sethiddenprop = sethiddenproperty
getgenv().gethiddenprop = gethiddenproperty

getgenv().isluaclosure = function(f)
    assert(type(f) == "function", "invalid argument #1 to 'isluaclosure' (function expected, got " .. type(f) .. ")")
    if type(iscclosure) == "function" then
        return not iscclosure(f)
    end
    local ok, info = pcall(debug.getinfo, f, "s")
    return ok and info.what ~= "C"
end

getgenv().run_on_actors = function(fn_or_source, ...)
    local actors = getgenv().getactors()
    local results = {}
    for _, actor in ipairs(actors) do
        table.insert(results, { getgenv().run_on_actor(actor, fn_or_source, ...) })
    end
    return results
end

if type(websocket) == "table" then
    if type(websocket.connect) == "function" then
        websocket.Connect = websocket.connect
    end
    getgenv().WebSocket = websocket
    getgenv().websockets = websocket
elseif type(WebSocket) == "table" then
    if type(WebSocket.connect) == "function" then
        WebSocket.Connect = WebSocket.connect
    end
    getgenv().websocket = WebSocket
    getgenv().websockets = WebSocket
end

getgenv().consolename = setconsolename
getgenv().consoleinfo = function(...) if type(console) == "table" and type(console.info) == "function" then return console.info(...) end end
getgenv().consolewarn = function(...) if type(console) == "table" and type(console.warn) == "function" then return console.warn(...) end end
getgenv().consoleerror = function(...) if type(console) == "table" and type(console.error) == "function" then return console.error(...) end end
getgenv().rconsoleerror = rconsoleerr
getgenv().savegame = saveinstance
getgenv().consolename = rconsolename or setconsolename
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

local execName = "Ryxzve"
local execVer = "V6.9.7"

getgenv().identifyexecutor = function()
    return execName, execVer
end

getgenv().getexecutorname = function()
    return execName
end

getgenv().getexecutorversion = function()
    return execVer
end

local oldRequest = request
getgenv().request = function(options)
    if type(options) ~= "table" then
        error("request: options must be a table")
    end
    options.Headers = options.Headers or {}
    options.Headers["User-Agent"] = execName
    return oldRequest(options)
end

getgenv().http.request = getgenv().request
getgenv().http_request = getgenv().request

local HttpService = game:GetService("HttpService")

local HttpService = game:GetService("HttpService")

getgenv().HttpGet = function(url, returnRaw)
    assert(type(url) == "string", "invalid argument #1 to 'HttpGet' (string expected, got " .. type(url) .. ") ", 2)
    returnRaw = returnRaw == nil and true or returnRaw

    local result = request({
        Url = url,
        Method = "GET",
        Headers = {
            ["User-Agent"] = "Roblox/WinInet"
        }
    })

    if returnRaw then
        return result.Body
    end
    return HttpService:JSONDecode(result.Body)
end

getgenv().HttpPost = function(url, body, contentType)
    assert(type(url) == "string", "invalid argument #1 to 'HttpPost' (string expected, got " .. type(url) .. ") ", 2)
    local result = request({
        Url = url,
        Method = "POST",
        Body = body or "",
        Headers = { ["Content-Type"] = contentType or "application/json" }
    })
    return result.Body
end
do
    local DrawingCache = {}
    local DrawingObjects = {}

    local Fonts = {
        [0] = Enum.Font.Arial,
        [1] = Enum.Font.BuilderSans,
        [2] = Enum.Font.Gotham,
        [3] = Enum.Font.RobotoMono
    }

    local function get_ui_parent()
        local ok, coreGui = pcall(function() return game:GetService("CoreGui") end)
        if ok and coreGui then
            return coreGui
        end

        local Players = game:GetService("Players")
        local localPlayer = Players.LocalPlayer
        
        if not localPlayer then
            while not Players.LocalPlayer do
                task.wait(0.1)
            end
            localPlayer = Players.LocalPlayer
        end

        local ok2, playerGui = pcall(function() return localPlayer:WaitForChild("PlayerGui", 10) end)
        if ok2 and playerGui then
            return playerGui
        end

        return game:GetService("Workspace")
    end

    local cg = get_ui_parent()

    local hui
    local ok, existing = pcall(function() return cg:FindFirstChild("HiddenUI") end)
    if ok and existing and existing:IsA("Folder") then
        hui = existing
    else
        hui = Instance.new("Folder")
        hui.Name = "HiddenUI"
        pcall(function() hui.Parent = cg end)
    end

    getgenv().gethui = function() return hui end
    getgenv().get_hidden_gui = getgenv().gethui

    local safe_gethui = gethui or getgenv().gethui
    local uiParent = cg
    if safe_gethui then
        local success, result = pcall(safe_gethui)
        if success and result then
            uiParent = result
        end
    end

    local UI = Instance.new("ScreenGui")
    UI.Name = "DrawingLib"
    UI.DisplayOrder = 999999
    UI.IgnoreGuiInset = true
    UI.ResetOnSpawn = false
    UI.Parent = uiParent

    local Drawing = {
        Fonts = {
            UI = 0,
            System = 1,
            Plex = 2,
            Monospace = 3
        }
    }

    Drawing.new = function(drawingType)
        if type(drawingType) ~= "string" then
            error("Drawing.new: argument must be a string, got " .. type(drawingType), 2)
        end

        local validTypes = {
            Line = true,
            Square = true,
            Rectangle = true,
            Circle = true,
            Text = true,
            Image = true,
            Triangle = true,
            Quad = true
        }
        
        if not validTypes[drawingType] then
            error("Drawing.new: invalid drawing type '" .. drawingType .. "'", 2)
        end

        local drawingObj = Instance.new("Frame")
        drawingObj.BackgroundTransparency = 1
        drawingObj.BorderSizePixel = 0
        drawingObj.BackgroundColor3 = Color3.new(0, 0, 0) 
        drawingObj.Size = UDim2.fromOffset(0, 0)
        drawingObj.Position = UDim2.fromOffset(0, 0)
        drawingObj.Parent = UI
        
        local bgGradient = Instance.new("UIGradient")
        bgGradient.Color = ColorSequence.new(Color3.new(1, 1, 1))
        bgGradient.Parent = drawingObj

        local properties = {
            Visible = true,
            Color = Color3.new(1, 1, 1),
            Transparency = 1,
            ZIndex = 1
        }
        
        local applyColor = function(color)
            bgGradient.Color = ColorSequence.new(color)
        end
        
        local self = newproxy(true)
        local mt = getmetatable(self)
        
        local removed = false
        
        mt.__index = function(_, key)
            if key == "__OBJECT_EXISTS" then
                return not removed
            end
            return properties[key]
        end
        
        mt.__newindex = function(_, key, value)
            if removed then return end
            
            if key == "__OBJECT_EXISTS" then
                return
            end
            
            properties[key] = value
            
            if key == "Visible" then
                drawingObj.Visible = value
            elseif key == "Color" then
                applyColor(value)
            elseif key == "Transparency" then
                drawingObj.BackgroundTransparency = 1 - value
            elseif key == "ZIndex" then
                drawingObj.ZIndex = value
            end
        end
        
        mt.__tostring = function()
            return "Drawing"
        end
        
        mt.__metatable = "The metatable is locked"
        
        properties.Remove = function()
            if removed then return end
            
            removed = true
            
            if drawingObj and drawingObj.Parent then
                drawingObj:Destroy()
            end
            
            for i, obj in ipairs(DrawingCache) do
                if obj.proxy == self then
                    table.remove(DrawingCache, i)
                    break
                end
            end
            
            DrawingObjects[self] = nil
        end
        
        properties.Destroy = properties.Remove
        
        table.insert(DrawingCache, {proxy = self, instance = drawingObj})
        DrawingObjects[self] = true
        
        if drawingType == "Line" then
            drawingObj.AnchorPoint = Vector2.new(0.5, 0.5)
            
            properties.From = Vector2.zero
            properties.To = Vector2.zero
            properties.Thickness = 1
            
            local updateLine = function()
                if removed then return end
                
                local from = properties.From
                local to = properties.To
                local dx = to.X - from.X
                local dy = to.Y - from.Y
                local length = math.sqrt(dx * dx + dy * dy)
                
                drawingObj.Size = UDim2.fromOffset(length, properties.Thickness)
                drawingObj.Position = UDim2.fromOffset((from.X + to.X) / 2, (from.Y + to.Y) / 2)
                drawingObj.Rotation = math.deg(math.atan2(dy, dx))
                drawingObj.BackgroundTransparency = 1 - properties.Transparency
                applyColor(properties.Color)
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k == "From" or k == "To" or k == "Thickness" then
                    updateLine()
                end
            end
            
        elseif drawingType == "Square" or drawingType == "Rectangle" then
            local stroke = Instance.new("UIStroke")
            stroke.Parent = drawingObj
            stroke.Thickness = 1
            stroke.Color = Color3.new(1, 1, 1)
            
            properties.Size = Vector2.zero
            properties.Position = Vector2.zero
            properties.Filled = false
            properties.Thickness = 1
            
            local updateSquare = function()
                if removed then return end
                
                drawingObj.Size = UDim2.fromOffset(properties.Size.X, properties.Size.Y)
                drawingObj.Position = UDim2.fromOffset(properties.Position.X, properties.Position.Y)
                
                if properties.Filled then
                    drawingObj.BackgroundTransparency = 1 - properties.Transparency
                    applyColor(properties.Color)
                    stroke.Enabled = false
                else
                    drawingObj.BackgroundTransparency = 1
                    stroke.Enabled = true
                    stroke.Color = properties.Color
                    stroke.Thickness = properties.Thickness
                    stroke.Transparency = 1 - properties.Transparency
                end
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k == "Size" or k == "Position" or k == "Filled" or k == "Thickness" or k == "Color" or k == "Transparency" then
                    updateSquare()
                end
            end
            
        elseif drawingType == "Circle" then
            local stroke = Instance.new("UIStroke")
            stroke.Parent = drawingObj
            stroke.Thickness = 1
            
            local corner = Instance.new("UICorner")
            corner.CornerRadius = UDim.new(1, 0)
            corner.Parent = drawingObj
            
            properties.Radius = 50
            properties.Position = Vector2.zero
            properties.Filled = false
            properties.Thickness = 1
            properties.NumSides = 64
            
            local updateCircle = function()
                if removed then return end
                
                local diameter = properties.Radius * 2
                drawingObj.Size = UDim2.fromOffset(diameter, diameter)
                drawingObj.Position = UDim2.fromOffset(properties.Position.X - properties.Radius, properties.Position.Y - properties.Radius)
                
                if properties.Filled then
                    drawingObj.BackgroundTransparency = 1 - properties.Transparency
                    applyColor(properties.Color)
                    stroke.Enabled = false
                else
                    drawingObj.BackgroundTransparency = 1
                    stroke.Enabled = true
                    stroke.Color = properties.Color
                    stroke.Thickness = properties.Thickness
                    stroke.Transparency = 1 - properties.Transparency
                end
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k == "Radius" or k == "Position" or k == "Filled" or k == "Thickness" or k == "Color" or k == "Transparency" then
                    updateCircle()
                end
            end
            
        elseif drawingType == "Triangle" then
            drawingObj:Destroy()
            
            drawingObj = Instance.new("Frame")
            drawingObj.BackgroundTransparency = 1
            drawingObj.BorderSizePixel = 0
            drawingObj.BackgroundColor3 = Color3.new(0, 0, 0)
            drawingObj.Size = UDim2.fromOffset(100, 100)
            drawingObj.Parent = UI
            
            bgGradient = Instance.new("UIGradient")
            bgGradient.Color = ColorSequence.new(properties.Color)
            bgGradient.Parent = drawingObj
            
            local function createPointFrame()
                local f = Instance.new("Frame")
                f.AnchorPoint = Vector2.new(0.5, 0.5)
                f.BackgroundColor3 = Color3.new(0, 0, 0)
                f.BorderSizePixel = 0
                local g = Instance.new("UIGradient")
                g.Color = ColorSequence.new(properties.Color)
                g.Parent = f
                return f, g
            end
            
            local point1Frame, grad1 = createPointFrame()
            point1Frame.Parent = drawingObj
            local point2Frame, grad2 = createPointFrame()
            point2Frame.Parent = drawingObj
            local point3Frame, grad3 = createPointFrame()
            point3Frame.Parent = drawingObj
            
            properties.PointA = Vector2.zero
            properties.PointB = Vector2.new(50, 100)
            properties.PointC = Vector2.new(100, 0)
            properties.Filled = false
            properties.Thickness = 1
            
            local updateTriangle = function()
                if removed then return end
                
                local pA = properties.PointA
                local pB = properties.PointB
                local pC = properties.PointC
                
                local minX = math.min(pA.X, pB.X, pC.X)
                local minY = math.min(pA.Y, pB.Y, pC.Y)
                local maxX = math.max(pA.X, pB.X, pC.X)
                local maxY = math.max(pA.Y, pB.Y, pC.Y)
                
                drawingObj.Position = UDim2.fromOffset(minX, minY)
                drawingObj.Size = UDim2.fromOffset(maxX - minX, maxY - minY)
                
                local function drawLine(frame, grad, p1, p2)
                    local dx = p2.X - p1.X
                    local dy = p2.Y - p1.Y
                    local length = math.sqrt(dx * dx + dy * dy)
                    
                    frame.Size = UDim2.fromOffset(length, properties.Thickness)
                    frame.Position = UDim2.fromOffset((p1.X + p2.X) / 2 - minX, (p1.Y + p2.Y) / 2 - minY)
                    frame.Rotation = math.deg(math.atan2(dy, dx))
                    grad.Color = ColorSequence.new(properties.Color)
                    frame.BackgroundTransparency = 1 - properties.Transparency
                end
                
                if properties.Filled then
                    point1Frame.Visible = false
                    point2Frame.Visible = false
                    point3Frame.Visible = false
                    bgGradient.Color = ColorSequence.new(properties.Color)
                    drawingObj.BackgroundTransparency = 1 - properties.Transparency
                else
                    point1Frame.Visible = true
                    point2Frame.Visible = true
                    point3Frame.Visible = true
                    drawingObj.BackgroundTransparency = 1
                    
                    drawLine(point1Frame, grad1, pA, pB)
                    drawLine(point2Frame, grad2, pB, pC)
                    drawLine(point3Frame, grad3, pC, pA)
                end
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k == "PointA" or k == "PointB" or k == "PointC" or k == "Filled" or k == "Thickness" or k == "Color" or k == "Transparency" then
                    updateTriangle()
                end
            end
            
        elseif drawingType == "Quad" then
            drawingObj:Destroy()
            
            drawingObj = Instance.new("Frame")
            drawingObj.BackgroundTransparency = 1
            drawingObj.BorderSizePixel = 0
            drawingObj.BackgroundColor3 = Color3.new(0, 0, 0)
            drawingObj.Size = UDim2.fromOffset(100, 100)
            drawingObj.Parent = UI
            
            bgGradient = Instance.new("UIGradient")
            bgGradient.Color = ColorSequence.new(properties.Color)
            bgGradient.Parent = drawingObj
            
            local function createLineFrame()
                local f = Instance.new("Frame")
                f.AnchorPoint = Vector2.new(0.5, 0.5)
                f.BackgroundColor3 = Color3.new(0, 0, 0)
                f.BorderSizePixel = 0
                local g = Instance.new("UIGradient")
                g.Color = ColorSequence.new(properties.Color)
                g.Parent = f
                return f, g
            end
            
            local line1, grad1 = createLineFrame()
            line1.Parent = drawingObj
            local line2, grad2 = createLineFrame()
            line2.Parent = drawingObj
            local line3, grad3 = createLineFrame()
            line3.Parent = drawingObj
            local line4, grad4 = createLineFrame()
            line4.Parent = drawingObj
            
            properties.PointA = Vector2.zero
            properties.PointB = Vector2.new(100, 0)
            properties.PointC = Vector2.new(100, 100)
            properties.PointD = Vector2.new(0, 100)
            properties.Filled = false
            properties.Thickness = 1
            
            local updateQuad = function()
                if removed then return end
                
                local pA = properties.PointA
                local pB = properties.PointB
                local pC = properties.PointC
                local pD = properties.PointD
                
                local minX = math.min(pA.X, pB.X, pC.X, pD.X)
                local minY = math.min(pA.Y, pB.Y, pC.Y, pD.Y)
                local maxX = math.max(pA.X, pB.X, pC.X, pD.X)
                local maxY = math.max(pA.Y, pB.Y, pC.Y, pD.Y)
                
                drawingObj.Position = UDim2.fromOffset(minX, minY)
                drawingObj.Size = UDim2.fromOffset(maxX - minX, maxY - minY)
                
                local function drawLine(frame, grad, p1, p2)
                    local dx = p2.X - p1.X
                    local dy = p2.Y - p1.Y
                    local length = math.sqrt(dx * dx + dy * dy)
                    
                    frame.Size = UDim2.fromOffset(length, properties.Thickness)
                    frame.Position = UDim2.fromOffset((p1.X + p2.X) / 2 - minX, (p1.Y + p2.Y) / 2 - minY)
                    frame.Rotation = math.deg(math.atan2(dy, dx))
                    grad.Color = ColorSequence.new(properties.Color)
                    frame.BackgroundTransparency = 1 - properties.Transparency
                end
                
                if properties.Filled then
                    line1.Visible = false
                    line2.Visible = false
                    line3.Visible = false
                    line4.Visible = false
                    bgGradient.Color = ColorSequence.new(properties.Color)
                    drawingObj.BackgroundTransparency = 1 - properties.Transparency
                else
                    line1.Visible = true
                    line2.Visible = true
                    line3.Visible = true
                    line4.Visible = true
                    drawingObj.BackgroundTransparency = 1
                    
                    drawLine(line1, grad1, pA, pB)
                    drawLine(line2, grad2, pB, pC)
                    drawLine(line3, grad3, pC, pD)
                    drawLine(line4, grad4, pD, pA)
                end
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k == "PointA" or k == "PointB" or k == "PointC" or k == "PointD" or k == "Filled" or k == "Thickness" or k == "Color" or k == "Transparency" then
                    updateQuad()
                end
            end
            
        elseif drawingType == "Text" then
            drawingObj:Destroy()
            
            drawingObj = Instance.new("TextLabel")
            drawingObj.BackgroundTransparency = 1
            drawingObj.BorderSizePixel = 0
            drawingObj.TextColor3 = Color3.new(1, 1, 1)
            drawingObj.TextSize = 13
            drawingObj.Font = Enum.Font.Code
            drawingObj.Text = ""
            drawingObj.AutomaticSize = Enum.AutomaticSize.XY
            drawingObj.Parent = UI
            
            applyColor = function(color)
                drawingObj.TextColor3 = color
            end
            
            properties.Text = ""
            properties.Size = 13
            properties.Center = false
            properties.Outline = false
            properties.OutlineColor = Color3.new(0, 0, 0)
            properties.Position = Vector2.zero
            properties.Font = 2
            properties.TextBounds = Vector2.zero
            
            local updateText = function()
                if removed then return end
                
                drawingObj.Text = tostring(properties.Text)
                drawingObj.TextSize = properties.Size
                drawingObj.Position = UDim2.fromOffset(properties.Position.X, properties.Position.Y)
                applyColor(properties.Color)
                drawingObj.TextTransparency = 1 - properties.Transparency
                drawingObj.Font = Fonts[properties.Font] or Enum.Font.Code
                
                if properties.Center then
                    drawingObj.TextXAlignment = Enum.TextXAlignment.Center
                    drawingObj.TextYAlignment = Enum.TextYAlignment.Center
                else
                    drawingObj.TextXAlignment = Enum.TextXAlignment.Left
                    drawingObj.TextYAlignment = Enum.TextYAlignment.Top
                end
                
                if properties.Outline then
                    drawingObj.TextStrokeTransparency = 0
                    drawingObj.TextStrokeColor3 = properties.OutlineColor
                else
                    drawingObj.TextStrokeTransparency = 1
                end
                
                game:GetService("RunService").RenderStepped:Wait()
                properties.TextBounds = drawingObj.TextBounds
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k ~= "TextBounds" then
                    task.spawn(updateText)
                end
            end
            
        elseif drawingType == "Image" then
            drawingObj:Destroy()
            
            drawingObj = Instance.new("ImageLabel")
            drawingObj.BackgroundTransparency = 1
            drawingObj.BorderSizePixel = 0
            drawingObj.Parent = UI
            
            applyColor = function(color)
                drawingObj.ImageColor3 = color
            end
            
            properties.Data = ""
            properties.Size = Vector2.zero
            properties.Position = Vector2.zero
            properties.Rounding = 0
            
            local updateImage = function()
                if removed then return end
                
                drawingObj.Image = properties.Data
                drawingObj.Size = UDim2.fromOffset(properties.Size.X, properties.Size.Y)
                drawingObj.Position = UDim2.fromOffset(properties.Position.X, properties.Position.Y)
                drawingObj.ImageTransparency = 1 - properties.Transparency
                applyColor(properties.Color)
            end
            
            local oldNewindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                oldNewindex(t, k, v)
                if k == "Data" or k == "Size" or k == "Position" or k == "Transparency" or k == "Color" then
                    updateImage()
                end
            end
        end
        
        return self
    end

    getgenv().Drawing = Drawing

    getgenv().isrenderobj = function(obj)
        if type(obj) ~= "userdata" then
            return false
        end
        
        local success, exists = pcall(function()
            return obj.__OBJECT_EXISTS
        end)
        
        return success and exists == true
    end

    getgenv().cleardrawcache = function()
        for i = #DrawingCache, 1, -1 do
            local data = DrawingCache[i]
            if data and data.proxy then
                pcall(function()
                    data.proxy:Remove()
                end)
            end
        end
        
        DrawingCache = {}
        DrawingObjects = {}
    end

    getgenv().getrenderproperty = function(obj, prop)
        if not getgenv().isrenderobj(obj) then
            return nil
        end
        return obj[prop]
    end

    getgenv().setrenderproperty = function(obj, prop, value)
        if not getgenv().isrenderobj(obj) then
            return
        end
        obj[prop] = value
    end
end
