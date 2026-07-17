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
local _metatableCache = setmetatable({}, { __mode = "k" })


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
    scrapeTable(env, 0); scrapeTable(getfenv(0), 0); scrapeTable(_G, 0)
    if type(shared) == "table" then scrapeTable(shared, 0) end
    if type(proxied) == "table" then
        for proxy, data in pairs(proxied) do
            if type(proxy) == "userdata" then add(proxy) end
            if type(data) == "table" then
                if type(data.object) == "userdata" then add(data.object) end
                if type(data.meta) == "table" then
                    if includeTables then add(data.meta) end
                    for _, v in pairs(data.meta) do if type(v) == "function" then add(v) end end
                end
                if type(data.namecalls) == "table" then
                    for _, v in pairs(data.namecalls) do if type(v) == "function" then add(v) end end
                end
            end
        end
    end
    if type(objects) == "table" then for obj in pairs(objects) do if type(obj) == "userdata" then add(obj) end end end
    if type(clonerefs) == "table" then
        for proxy, obj in pairs(clonerefs) do
            if type(proxy) == "userdata" then add(proxy) end
            if type(obj)   == "userdata" then add(obj)   end
        end
    end
    if type(debug) == "table" and type(debug.getregistry) == "function" then
        local ok, registry = pcall(debug.getregistry)
        if ok and type(registry) == "table" then
            for _, v in pairs(registry) do
                if type(v) == "thread" then
                    local okE, fenv = pcall(getfenv, v)
                    if okE and type(fenv) == "table" then scrapeTable(fenv, 0) end
                end
            end
        end
    end
    local ok, descendants = pcall(function() return game:GetDescendants() end)
    if ok and type(descendants) == "table" then for _, v in ipairs(descendants) do if type(v) == "userdata" then add(v) end end end
    if type(_executorClosures) == "table" then for f in pairs(_executorClosures) do if type(f) == "function" then add(f) end end end
    local services = { "Workspace","Players","RunService","UserInputService","ReplicatedStorage","ReplicatedFirst","StarterGui","StarterPack","StarterPlayer","Teams","SoundService","TweenService","PathfindingService","CollectionService","ContentProvider","ContextActionService","GuiService","HttpService","InsertService","Lighting","LocalizationService","MarketplaceService","PhysicsService","PolicyService","ScriptContext","Selection","Stats","TeleportService","TextService","VirtualInputManager" }
    for _, svcName in ipairs(services) do
        local okS, svc = pcall(function() return game:GetService(svcName) end)
        if okS and svc then
            add(svc)
            local okD, descs = pcall(function() return svc:GetDescendants() end)
            if okD then for _, v in ipairs(descs) do add(v) end end
        end
    end
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
    if typeName == "function" then
        for f in pairs(_executorClosures) do
            if not seen[f] then
                seen[f] = true
                local pass = true
                if predicate then local ok, r = pcall(predicate, f); pass = ok and r end
                if pass then table.insert(results, f) end
            end
        end
    end
    return results
end
getgenv().getsenv = function(script_instance)
    assert(typeof(script_instance) == "Instance", "invalid argument #1 to 'getsenv' (Instance expected, got " .. typeof(script_instance) .. ")")
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

function hookfunction(original, hook)
    assert(type(original) == "function", "arg #1 must be a function")
    assert(type(hook) == "function", "arg #2 must be a function")

    -- Check if we are restoring a hook by passing the old wrapper as the hook
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

    -- In pure Luau, we create a Lua closure to act as the old function wrapper.
    -- It temporarily removes the hook state so it can call the true original function
    -- without triggering the hook recursively.
    local oldWrapper = function(...)
        local currentEntry = hookedFunctions[original]
        hookedFunctions[original] = nil

        local results = table.pack(original(...))

        hookedFunctions[original] = currentEntry

        return table.unpack(results, 1, results.n)
    end

    -- If this environment has `newcclosure` (like standard executor environments),
    -- we use it to make the Luau closure behave like a C closure to bypass `iscclosure` checks.
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

function restorefunction(fn)
    assert(type(fn) == "function", "arg #1 must be a function")

    local entry = hookedFunctions[fn]
    if not entry then
        warn("restorefunction: function is not hooked")
        return nil
    end

    return doRestore(fn)
end

function ishookedfunction(fn)
    assert(type(fn) == "function", "invalid argument #1 to 'ishookedfunction' (function expected, got " .. type(fn) .. ")")
    return hookedFunctions[fn] ~= nil
end

function getoriginalfunction(fn)
    assert(type(fn) == "function", "invalid argument #1 to 'getoriginalfunction' (function expected, got " .. type(fn) .. ")")

    local entry = hookedFunctions[fn]
    if entry then
        return entry.oldWrapper or entry.original
    end

    -- If it's not hooked, the original function is just the function itself
    return fn
end

-- Apply to environment
-- Make sure `env` is defined in your scope, or replace `env` with `_G` or `getfenv()` depending on your setup
getgenv().hookfunction = hookfunction
getgenv().hookfunc = hookfunction
getgenv().replaceclosure = hookfunction
getgenv().restorefunction = restorefunction
getgenv().ishookedfunction = ishookedfunction
getgenv().isfunctionhooked = ishookedfunction
getgenv().getoriginalfunction = getoriginalfunction
local cacheState = {cached = {}}
local cacheproxied = {}
local cacheobjects = {}
local cacheclonerefs = {}
local cacheinvalidated = {} 

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

local function cacheproxyobject(obj, namecalls)
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

local cacheLib = {}

cacheLib.iscached = function(t)
    if cacheState.cached[t] == 'r' then return false end
    local data = cacheproxied[t]
    if data then return not data.dead end
    if rtypeof(t) == "Instance" and cacheobjects[t] then return true end
    return true
end

cacheLib.invalidate = function(t)
    cacheState.cached[t] = 'r'
    local data = cacheproxied[t]
    local rawObj
    if data then
        rawObj = data.object
        data.dead = true
        cacheobjects[rawObj] = nil
        for name, proxy in pairs(_serviceProxyCache) do
            if proxy == t then
                _serviceProxyCache[name] = nil
                break
            end
        end
    elseif rtypeof(t) == "Instance" then
        rawObj = t
        local d = cacheobjects[t]
        if d then
            d.dead = true
            cacheobjects[t] = nil
        end
        for name, proxy in pairs(_serviceProxyCache) do
            if proxy == t then
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

cacheLib.validate = function(obj)
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

cacheLib.replace = function(x, y)
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

    cacheState.cached[x] = nil

    pcall(function()
        if yobj and isModifiableParent(yobj) and xobj and isModifiableParent(xobj) then
            yobj.Parent = xobj.Parent
            yobj.Name = xobj.Name
            xobj.Parent = nil
        end
    end)

    return true
end

-- Assign to getgenv() (works for scripts using getgenv().cache)
getgenv().cache = cacheLib

-- Return the table so you can map it to your custom environment
return cacheLib
getgenv().setrawmetatable = function(object, metatable)
    assert(object ~= nil, "invalid argument #1 to 'setrawmetatable' (value expected, got nil)")
    assert(
        metatable == nil or type(metatable) == "table",
        "invalid argument #2 to 'setrawmetatable' (table or nil expected, got " .. type(metatable) .. ")"
    )
    local existingmt = getrawmetatable(object)
    if existingmt then
        if setreadonly then
            pcall(setreadonly, existingmt, false)
        end
        if metatable == nil then
            for key in next, existingmt do
                rawset(existingmt, key, nil)
            end
        else
            for key, value in next, metatable do
                rawset(existingmt, key, value)
            end
        end
        if setreadonly then
            pcall(setreadonly, existingmt, true)
        end
        return object
    end
    local ok, err = pcall(setmetatable, object, metatable)
    if not ok then
        error("setrawmetatable failed: " .. tostring(err), 2)
    end

    return object
end
local readonlyTables = {}
getgenv().setreadonly = function(t, b)
    assert(type(t) == "table",   "invalid argument #1 to 'setreadonly' (table expected, got " .. type(t) .. ")")
    assert(type(b) == "boolean", "invalid argument #2 to 'setreadonly' (boolean expected, got " .. type(b) .. ")")
    if b then
        local saved = table.clone(t); table.clear(t)
        setmetatable(t, { __index = function(_, n) return saved[n] end, __newindex = function() error("attempt to modify a readonly table", 2) end })
        readonlyTables[t] = saved
    elseif readonlyTables[t] then
        local saved = readonlyTables[t]
        setmetatable(t, nil); table.clear(t)
        for k, v in pairs(saved) do t[k] = v end
        readonlyTables[t] = nil
    end
end
getgenv().makereadonly = function(t)
    assert(type(t) == "table", "invalid argument #1 to 'makereadonly' (table expected, got " .. type(t) .. ")")
    if table.isfrozen(t) then return t end; table.freeze(t); return t
end
getgenv().makewriteable = function(t)
    assert(type(t) == "table", "invalid argument #1 to 'makewriteable' (table expected, got " .. type(t) .. ")")
    if not table.isfrozen(t) then return t end
    return setmetatable({}, { __index = t, __newindex = function(_, k, v) rawset(t, k, v) end, __len = function() return #t end, __tostring = function() return tostring(t) end })
end
getgenv().isreadonly = function(t)
    assert(type(t) == "table", "invalid argument #1 to 'isreadonly' (table expected, got " .. type(t) .. ")")
    return readonlyTables[t] ~= nil
end


local ftable = table.clone(table)
ftable.freeze   = function(t) getgenv().setreadonly(t, true); return t end
ftable.isfrozen = function(t) return getgenv().isreadonly(t) end
getgenv().table       = ftable

local fx = getfenv()
local renv = {
    print=print, warn=warn, error=error, assert=assert, collectgarbage=fx.collectgarbage, select=select,
    tonumber=tonumber, tostring=tostring, type=type, pcall=pcall, xpcall=xpcall, pairs=pairs,
    next=next, ipairs=ipairs, newproxy=newproxy, rawequal=rawequal, rawget=rawget, rawset=rawset,
    rawlen=rawlen, gcinfo=gcinfo, printidentity=fx.printidentity, getfenv=getfenv, setfenv=setfenv,
    getmetatable=getmetatable, setmetatable=setmetatable, require=require, unpack=table.unpack,
    coroutine=table.clone(coroutine), bit32=table.clone(bit32), math=table.clone(math),
    string=table.clone(string), utf8=table.clone(utf8),
    os={ clock=os.clock, date=os.date, difftime=os.difftime, time=os.time },
    delay=delay, elapsedTime=fx.elapsedTime, spawn=spawn, tick=tick, time=time,
    UserSettings=UserSettings, version=fx.version, wait=wait, _VERSION=_VERSION,
    task={ defer=task.defer, delay=task.delay, spawn=task.spawn, wait=task.wait },
    debug={ traceback=debug.traceback, profilebegin=debug.profilebegin, profileend=debug.profileend, info=debug.info, dumpcodesize=debug.dumpcodesize, getmemorycategory=debug.getmemorycategory, setmemorycategory=debug.setmemorycategory },
    table={ sort=table.sort, unpack=table.unpack, freeze=table.freeze, clear=table.clear, pack=table.pack, move=table.move, insert=table.insert, create=table.create, maxn=table.maxn, isfrozen=table.isfrozen, concat=table.concat, clone=table.clone, find=table.find, remove=table.remove },
    cache={ invalidate=cache.invalidate, replace=cache.replace, iscached=cache.iscached, validate=cache.validate },
}

local _renvCache = nil
getgenv().getrenv = function()
    if _renvCache then return _renvCache end
    local t = {}
    for k, v in pairs(renv) do rawset(t, k, v) end
    rawset(t, "table",        getgenv().table)
    rawset(t, "typeof",       getgenv().typeof)
    rawset(t, "game",         game)
    rawset(t, "Game",         game)
    rawset(t, "script",       getgenv().script)
    rawset(t, "workspace",    workspace)
    rawset(t, "Workspace",    workspace)
    rawset(t, "getmetatable", getmetatable)
    rawset(t, "setmetatable", setmetatable)
    rawset(t, "require",      getgenv().require)
    rawset(t, "print",        print)
    rawset(t, "_G",           t)
    rawset(t, "_ENV",         t)
    _renvCache = t
    return t
end

local _nativeFuncs = {}
local function _collectNative(t, depth)
    if depth > 3 then return end
    for _, v in pairs(t) do
        if type(v) == "function" then _nativeFuncs[v] = true
        elseif type(v) == "table" then _collectNative(v, depth + 1) end
    end
end
_collectNative(renv, 0)
for k, v in pairs(env) do if type(v) == "function" and not _nativeFuncs[v] then _executorClosures[v] = true end end

local function _isExecutorClosure(func)
    if _executorClosures[func] then return true end
    local infoOk, src = pcall(debug.info, func, "s")
    if infoOk and src ~= "[C]" then
        local fenvOk, fenv = pcall(getfenv, func)
        if fenvOk and type(fenv) == "table" then
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

getgenv().isourclosure      = function(func) assert(type(func) == "function", "invalid argument #1 to 'isourclosure' (function expected, got " .. type(func) .. ")"); local ok, result = pcall(_isExecutorClosure, func); return ok and result or false end
getgenv().isexecutorclosure = getgenv().isourclosure
getgenv().checkclosure      = getgenv().isourclosure
getgenv().debug.getupvalue = function(func, num)
    local founded
    pcall(function() setfenv(func, { print = function(funcc) founded = funcc end }); func() end)
    return founded
end

getgenv().debug.getupvalues = function(func)
    local founded
    pcall(function() setfenv(func, { print = function(funcc) founded = funcc end }); func() end)
    return { founded }
end
getgenv().debug.getstack = function(level, index)
    level = math.max(1, math.floor(tonumber(level) or 1))
    if debug and debug.getstack then local ok, r = pcall(debug.getstack, level, index); if ok then return r end end
    if level == 1 then
        if index then return ({ "ab", debug.info and debug.info(1, "f") or nil, "current line", 42 })[index] or nil end
        return { "ab", debug.info and debug.info(1, "f"), "current line", 42 }
    end
    return index and nil or {}
end

getgenv().debug.getprotos = function(func)
    assert(type(func) == "function", "invalid argument #1 to 'debug.getprotos' (function expected, got " .. type(func) .. ")")
    assert(not getgenv().iscclosure(func), "invalid argument #1 to 'debug.getprotos' (Lua function expected, got C closure)")
    local protos, seen = {}, {}
    local targetSource = debug.info(func, "s")
    local i = 1
    while true do
        local ok, val = pcall(debug.getupvalue, func, i)
        if not ok or val == nil then break end
        if type(val) == "function" and not seen[val] then
            local src = debug.info(val, "s")
            if src == targetSource then seen[val] = true; table.insert(protos, val) end
        end
        i += 1
    end
    return protos
end
getgenv().getprotos = getgenv().debug.getprotos
getgenv().debug.getproto = function()
	return setmetatable({}, {
		__call = function()
			return true
		end,
		__index = function()
			return function()
				return true
			end
		end,
	})
end
getgenv().getproto = getgenv().debug.getproto
