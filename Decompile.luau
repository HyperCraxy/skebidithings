assert(getscriptbytecode, "U arent meant to see ts.")

local HttpService = game:GetService("HttpService")

_G.DecompilerKey = _G.DecompilerKey or nil
-- These are automatically added to the request URL.
_G.DecompilerOptions = _G.DecompilerOptions or {
    type_annotations = "functions",
    generated_names = "readable",
    indent = "tab",
}

local API_URL = "https://api.6-7.wtf/decompile"
local MAX_RETRIES = 3

local function errorMessage(title, detail)
    local text = tostring(detail or "unknown error"):gsub("\r\n?", "\n")
    return "-- " .. title .. "\n-- " .. text:gsub("\n", "\n-- ")
end

local function buildUrl()
    local fields = {}
    for name, value in pairs(_G.DecompilerOptions) do
        fields[#fields + 1] = HttpService:UrlEncode(tostring(name))
            .. "="
            .. HttpService:UrlEncode(tostring(value))
    end
    if #fields == 0 then
        return API_URL
    end
    return API_URL .. "?" .. table.concat(fields, "&")
end

getgenv().decompile = function(scr)
    local ok, bytecode = pcall(getscriptbytecode, scr)
    if not ok then
        return errorMessage("failed to read script bytecode", bytecode)
    end

    local headers = { ["Content-Type"] = "application/octet-stream" }
    if _G.DecompilerKey and _G.DecompilerKey ~= "" then
        headers.Authorization = "Bearer " .. _G.DecompilerKey
    end

    local url = buildUrl()

    for attempt = 0, MAX_RETRIES do
        local sent, res = pcall(request, {
            Url = url,
            Method = "POST",
            Headers = headers,
            Body = bytecode,
        })

        local status = sent and res and res.StatusCode
        if status == 200 then
            return res.Body
        end

        local retryable = not status or status == 429 or status >= 500
        if attempt == MAX_RETRIES or not retryable then
            return errorMessage(
                "decompiler API request failed",
                status and ("HTTP " .. status .. "\n" .. tostring(res.Body)) or res
            )
        end

        local resHeaders = res and res.Headers or {}
        local delay = tonumber(resHeaders["Retry-After"] or resHeaders["retry-after"]) or 2 ^ attempt
        warn(string.format("[6-7.wtf] Request failed; retrying in %.1f seconds", delay))
        task.wait(delay)
    end
end
