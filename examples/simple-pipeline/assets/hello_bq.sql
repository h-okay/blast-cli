/* @blast

name: dashboard.hello_bq
type: bq.sql

depends:
   - hello_python

materialization:
   type: table

columns:
   one:
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]


@blast */

select 1 as one
union all
select 2 as one
