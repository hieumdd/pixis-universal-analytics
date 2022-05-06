from universal_analytics.pipeline import call, call_campaign_dial, contact, customer, contact_count

pipelines = {
    i.name: i
    for i in [
        call_campaign_dial.pipeline,
        contact.pipeline,
        customer.pipeline,
        # call.pipeline("Call_Inbound", {"direction": 1}),
        call.pipeline("Call_Outbound", {"direction": 2}),
        # call.pipeline("Call_Internal", {"direction": 3}),
        contact_count.pipeline,
    ]
}
