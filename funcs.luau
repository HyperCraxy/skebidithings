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

assert(getscriptbytecode, "Ur Eggsploit is shit womp womp.")
local last = 0
local httpservice = cloneref and cloneref(game:GetService("HttpService")) or game:GetService("HttpService")
getgenv().decompile = function(scr)
    local ok, bytecode = pcall(getscriptbytecode, scr)
    if not ok then
        return "-- failed to read script bytecode\n--[[\n" .. tostring(bytecode) .. "\n--]]"
    end

    local elapsed = os.clock() - last
    if elapsed < 0.12 then
        task.wait(0.12 - elapsed)
    end

    local encoder = base64_encode
    if not encoder then
        encoder = function(data)
            local b = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
            return ((data:gsub('.', function(x)
                local r,byte = '',x:byte()
                for i=8,1,-1 do
                    r = r .. (byte % 2^i - byte % 2^(i-1) > 0 and '1' or '0')
                end
                return r
            end)..'0000'):gsub('%d%d%d?%d?%d?%d?', function(x)
                if #x < 6 then return '' end
                local c = 0
                for i=1,6 do
                    c = c + (x:sub(i,i) == '1' and 2^(6-i) or 0)
                end
                return b:sub(c+1,c+1)
            end)..({ '', '==', '=' })[#data % 3 + 1])
        end
    end

    local res = request({
        Url = "https://api.lua.expert/decompile",
        Method = "POST",
        Headers = {
            ["content-type"] = "application/json"
        },
        Body = httpservice:JSONEncode({
            script = encoder(bytecode)
        })
    })

    last = os.clock()

    if not res or res.StatusCode ~= 200 then
        return "-- api request error\n--[[\n" .. (res and res.Body or "no response") .. "\n--]]"
    end

    return res.Body
end
