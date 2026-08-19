-- Real-time change detection and webhook notifications (v8.0)

local Webhooks = {
    subscribers = {},
}

function Webhooks.subscribe(endpoint, event_types)
    table.insert(Webhooks.subscribers, {
        endpoint = endpoint,
        events = event_types or {"*"},
    })
    return true
end

function Webhooks.notify(event_type, data)
    local delivered = 0
    for _, subscriber in ipairs(Webhooks.subscribers) do
        for _, event in ipairs(subscriber.events) do
            if event == "*" or event == event_type then
                -- In production: HTTP POST to subscriber.endpoint
                delivered = delivered + 1
            end
        end
    end
    return {event = event_type, delivered = delivered}
end

function Webhooks.detect_changes(party_id, old_data, new_data)
    local changes = {}
    for key, old_value in pairs(old_data) do
        if new_data[key] ~= old_value then
            changes[key] = {
                old = old_value,
                new = new_data[key],
            }
        end
    end
    return changes
end

return Webhooks
