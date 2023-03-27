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
    tests:
        - name: not_null


@blast */

select 1 as one
union all
select 2 as one
