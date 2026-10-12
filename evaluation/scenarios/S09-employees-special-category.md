# S09 · Employees with health data (special category)

**Type:** new dataset · **Source:** Postgres `hris.public.employees` via Debezium · **Difficulty:** medium

Columns: `employee_id`, `full_name`, `work_email`, `department`, `job_title`,
`hired_date`, `sick_leave_days_ytd`, `has_disability_accommodation`, `updated_at`.

HR analytics needs headcount and turnover per department. Nobody outside HR may
see health information. Legal basis: employment contract and legal obligation.

**Done when:** contract merged, generated tests green.
