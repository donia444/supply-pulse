-- Compares the QA-gate decision in int_vendor_items__events with the answer key
-- (scenario_events.csv: expected PASS / REJECT and reason per CDC event).
-- Perfect result: every row has result_ok = reason_ok = event_time_ok = true.
-- Rows are grouped, so mismatches show which scenario / reason needs attention.

with expected as (

    select
        try_to_number(lsn)                                                  as lsn,
        vendor_id,
        item_ref,
        scenario,
        expected_gate_result,
        nullif(reject_reason, '')                                           as expected_reason,
        try_to_timestamp_ntz(replace(replace(event_time_utc, 'T', ' '), 'Z', '')) as expected_event_time
    from {{ source('answer_key', 'scenario_events') }}

),

ours as (

    select lsn, vendor_id, item_ref, gate_result, reject_reason, event_time_utc
    from {{ ref('int_vendor_items__events') }}

)

select
    coalesce(e.scenario, '(not in answer key)')                             as scenario,
    e.expected_gate_result,
    o.gate_result                                                           as our_gate_result,
    e.expected_reason,
    o.reject_reason                                                         as our_reason,
    equal_null(e.expected_gate_result, o.gate_result)                       as result_ok,
    equal_null(e.expected_reason, o.reject_reason)                          as reason_ok,
    equal_null(e.expected_event_time, o.event_time_utc)                     as event_time_ok,
    count(*)                                                                as events
from expected as e
full outer join ours as o
    on  e.lsn = o.lsn
    and e.vendor_id = o.vendor_id
    and e.item_ref = o.item_ref
group by all
order by result_ok, reason_ok, events desc
