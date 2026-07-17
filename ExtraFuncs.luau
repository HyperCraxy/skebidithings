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
        for _, v in ipairs(_realGame:GetDescendants()) do
            if v.ClassName == "Actor" then
                out[#out + 1] = ToProxy(v)
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
local envAddr = getptr(env)
if markerAddr and envAddr then
    Bridge("register_trusted", { 
        marker = string.format("%X", markerAddr), 
        env = string.format("%X", envAddr) 
    })
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

    local wasReadOnly = getgenv().isreadonly and getgenv().isreadonly(targetMt)
    if wasReadOnly then getgenv().setreadonly(targetMt, false) end
    rawset(targetMt, metamethodName, wrappedHook)
    if wasReadOnly then getgenv().setreadonly(targetMt, true) end

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
            "Triggered",
            "Activated",
            "Deactivated",
            "MouseClick",
            "MouseEnter",
            "MouseLeave",
            "Changed",
            "ChildAdded",
            "ChildRemoved",
            "DescendantAdded",
            "DescendantRemoved",
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
