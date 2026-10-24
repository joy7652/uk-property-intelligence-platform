-- The most recent cross-source verification verdict is a pass. It covers the transaction
-- series against the published index, which the marts' price series rest on. The crime
-- series rest on the total crossing the published category changes unmoved, and that is
-- not read here: the crime load evaluates it before writing, so a crime fact in Gold has
-- already passed it.
--
-- Read, never recomputed. A run that breaches writes its results and then fails, and so
-- does a run whose rules all passed but one never reported; neither is a pass. A run the
-- plan skipped writes no results, so the verdict before it stands. No verdict at all
-- fails.
with verdicts as (
    select run.run_id, run.started_ts, run.status
    from {{ source('quality', 'pipeline_run') }} as run
    where run.source = 'cross_source_verification'
        and exists (
            select 1
            from {{ source('quality', 'rule_result') }} as result
            where result.run_id = run.run_id
        )
),

latest as (
    select run_id, status
    from verdicts
    order by started_ts desc, run_id desc
    limit 1
)

select
    'rule breached' as finding,
    result.rule,
    result.scope,
    result.observed,
    result.lower_bound,
    result.upper_bound,
    latest.status as run_status
from {{ source('quality', 'rule_result') }} as result
inner join latest
    on result.run_id = latest.run_id
where not result.passed

union all

select
    'run did not succeed',
    cast(null as string),
    cast(null as string),
    cast(null as double),
    cast(null as double),
    cast(null as double),
    latest.status
from latest
where latest.status <> 'succeeded'

union all

select
    'no verdict recorded',
    cast(null as string),
    cast(null as string),
    cast(null as double),
    cast(null as double),
    cast(null as double),
    cast(null as string)
from (select 1 as one_row) as anchor
where not exists (select 1 from latest)
