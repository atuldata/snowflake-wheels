# ODFI ETL
For running loads into the DW.
 - Queries ODFI, load_state and odfi_etl_status tables by job_name.
 - Runs the external script to load into the DW
 - Records in the load_state and odfi_etl_status table upon success
> Requires `$APP_ROOT` set in ENV. This is where the locks, logs, output, conf, etc. will go.
> In addition to the conf and script requirements(See below) you will also need to initialize the job in the load_state table using the `odfi_etl bootstrap` action on the command line. See `odfi_etl bootstrap --help`

# ENV.yaml
Need the following:
```yaml
DB_CONNECTION:
    SNOWFLAKE:
        ...

ODFI_HOST:
ODFI_USER:
ODFI_PASS: 
```

# Config
Include a named yaml file for each job_name as `$APP_ROOT/jobs/odfi_jobs/<job_name>.yaml`

Example:
```yaml
# Name of the ODFI Feed
FEED_NAME: MyFeed

# Do we need to extract content topics from this data source? Default is false. Can leave out if false.
NEEDS_CONTENT_TOPICS: !!bool true

# Load state var used to tracking progress of the uploads.
LOAD_STATE_VAR: my_load_state_var_name

# Other load states to update. This is optional. The variable_value set is to the same as the LOAD_STATE_VAR.
AUXILIARY_LOAD_STATE_VARS: # Needs to be a list
    - aux_load_state_name_one
    - aux_load_state_name_two

# Other jobs that need to be completed for the same readable_interval before this job can run
DEPENDS_ON: # Needs to be a list
    - job1
    - job2

# This is a list of statements to run in order.
ETL_STATEMENTS:
    # Here is s normal SQL stmt to run
    - SQL statement to run

    # Here is a normal SQL stmt to run with a label
    - label: A title or brief description
      stmt: SQL statement to run

    # Here is a break check
    - label: A description for the check that will get logged if results are returned.
      break_check: Query that if returns reseults will be logged as errors. The ETL statement processing will stop here

    # Here is a continue check
    - label: A description for the check
      continue_check: SELECT True or False from a query.
      stmt: SQL stmt to run if continue check is true.

# If There is rollup needed for the job you need to provide following configuration
ROLLUP_CONFIG:
  OX_TRANSACTION_SUM:
    keys:
      - x_platform_id
      - publisher_account_nk
      - site_nk
      - p_channel
      - domain
      - category1
      - category2
      - p_coin
      - p_def_ad_width
      - p_def_ad_height
      - p_ad_sizes
      - country_code
      - p_req_deliv_medium
      - p_mobl_app_bundle
      - p_mobl_app_name
      - is_mkt
      - p_mkt_floor
      - p_mkt_floor_usd
      - p_mkt_natural_floor
      - p_mkt_natural_floor_usd
      - p_mkt_op
      - p_op_coin
      - p_op_mkt_floor
      - p_op_mkt_floor_usd
      - p_op_mkt_natural_floor
      - p_op_mkt_natural_floor_usd
      - a_platform_id
      - advertiser_account_nk
      - order_nk
      - line_item_nk
      - ad_nk
      - a_ad_size
      - a_coin
      - a_mkt_op
      - wintype
      - package_nk
      - deal_nk
      - x_bidder_elig
      - p_mapped_adunit_type
      - p_targetcpm
      - p_op_targetcpm
      - p_medium
      - u_mobl_dev_cat
      - is_mobile_app
      - p_adunit_presentation_format
      - p_adunit_presentation_subformat
      - x_gd_platform_id
      - x_gd_package_id
      - x_gd_deal_id
      - x_exchange_fee_applied
      - x_apply_pd_fees
      - deal_win_type
      - x_bidout
      - p_ext_supply_partner_id
      - p_ext_auction_type
      - p_ext_cp_currency_code
      - sales_channel_code
      - x_price_won
      - x_pd_fee_applied
      - x_rewarded
    metrics:
      - tot_requests
      - tot_discard_req
      - tot_mkt_elig_req
      - tot_mkt_req
      - tot_auctions
      - tot_fills
      - tot_impressions
      - tot_billable_impressions
      - tot_fallback
      - tot_clicks
      - tot_a_spend
      - tot_usd_a_spend
      - tot_p_revenue
      - tot_usd_p_revenue
      - tot_p_op_revenue
      - tot_usd_p_op_revenue
      - tot_p_mkt_operator_revenue
      - tot_usd_p_mkt_operator_revenue
      - tot_a_mkt_operator_revenue
      - tot_usd_a_mkt_operator_revenue
      - tot_count_p_mkt_floor
      - tot_sum_p_mkt_floor
      - tot_count_p_op_mkt_floor
      - tot_pub_external_raw_clearing_price
      - tot_all_clearing_price_count
      - tot_sum_p_op_mkt_floor
      - tot_view_conversions
      - tot_click_conversions
      - tot_raw_conversion_revenue
      - tot_raw_conversion_spend
      - tot_fp_mkt_elig_req
      - tot_p_gross_rev

    timezone: UTC
    time_rollups:
      # NOTE that these keys ("base", "day", "month") are simple "time_name" placeholders,
      # because we may roll up a table into multiple destination tables.  The "interval"
      # key below is the real time interval the rollup is for.
      # queue_next_rollup field stores the next "time_name" rollup job to queue.
      advertiser_day:
        affected:
          a_platform_id: platform_id
          advertiser_account_nk: account_nk
         #publisher_account_nk: account_nk
        keys:
        - advt_rollup_date
        - advt_date_sid
        source: ox_transaction_sum_hourly_fact
        table: advt_ox_transaction_sum_daily_fact
        # Timezone is "advertiser", "publisher", "platform" or "UTC"
        timezone: advertiser
        interval: day
        time_source: utc_timestamp
        time_destination: advt_rollup_date
        load_state_variable_name: grid_advt_transaction_sum_daily_fact_rollup_last_hour
        source_load_state_variable_name: grid_ox_transaction_sum_hourly_fact_last_hour_loaded
      platform_day:
        affected:
          x_platform_id: platform_id
         #publisher_account_nk: account_nk
        keys:
        - instance_rollup_date
        - instance_date_sid
        source: ox_transaction_sum_hourly_fact
        table: ox_transaction_sum_daily_fact_instancetz
        # Timezone is "advertiser", "publisher", "platform" or "UTC"
        timezone: platform
        interval: day
        time_source: utc_timestamp
        time_destination: instance_rollup_date
        load_state_variable_name: grid_ox_transaction_sum_daily_fact_instancetz_rollup_last_hour
        source_load_state_variable_name: grid_ox_transaction_sum_hourly_fact_last_hour_loaded
      day:
        affected:
          x_platform_id: platform_id
         #publisher_account_nk: account_nk
        keys:
        - utc_rollup_date
        - utc_date_sid
        source: ox_transaction_sum_hourly_fact
        table: ox_transaction_sum_daily_fact
        # Timezone is "advertiser", "publisher", "platform" or "UTC"
        timezone: UTC
        interval: day
        time_source: utc_timestamp
        time_destination: utc_rollup_date
        load_state_variable_name: grid_ox_transaction_sum_daily_rollup_last_hour
        source_load_state_variable_name: grid_ox_transaction_sum_hourly_fact_last_hour_loaded
        queue_next_rollup: month
      month:
        affected:
          x_platform_id: platform_id
          #publisher_account_nk: account_nk
        keys:
        - utc_rollup_date
        - utc_month_sid
        source: ox_transaction_sum_daily_fact
        table: ox_transaction_sum_monthly_fact
        timezone: UTC
        interval: month
        time_source: utc_rollup_date
        time_destination: utc_rollup_date
        load_state_variable_name: grid_ox_transaction_sum_monthly_rollup_last_hour
        source_load_state_variable_name: grid_ox_transaction_sum_daily_rollup_last_hour

```

# load_state Table
 - This table's variable name will include the `<job_name>`
 - The variable_value includes the `<readable_interval>`
# odfi_etl_status Table
This table contains all of the successfully run job/feed/serials.
> If you need to re-run a serial then you will need to remove from this table.

# Troubleshooting

 - If the uploads are hanging on missing staging data then run `odfi_etl verify_download -j <job_name>`. This should fix the problem. We should probably open a bug ticket to find the cause of the error if it comes to this.


See Also
--------

usage: odfi_etl {add_new,bootstrap,download,report,rollup,upload,verify_download} [-h|--help]

This is the entry point for all things odfi_etl related.
- positional arguments:
  {add_new,bootstrap,download,report,rollup,upload,verify_download}
                        What are we doing today? For help on an action use:
                        odfi_etl <action> -h|--help

- optional arguments:
-  -d, --debug           Set log level to DEBUG (default: False)
-  -e END_SERIAL, --end_serial END_SERIAL
                        The end of the range of serials to run for. If left
                        out this will run until the last available serial in
                        ODFI. (default: None)
-  -j JOB_NAME, --job_name JOB_NAME
                        Name of the job to run. (default: None)
-  -p PREVIEW_ROLLUP_QUEUE, --preview_rollup_queue PREVIEW_ROLLUP_QUEUE
                        Preview Rollup Queue, it will just log all the
                        information from the rollup_queue for the job
                        (default: None)
-  -r READABLE_INTERVAL_STR, --readable_interval_str READABLE_INTERVAL_STR
                        The readable_interval_str to run for. Should be
                        formatted one of the following: ['%Y-%m-%d_%H',
                        '%Y-%m-%d'] (default: None)
-  --rollup_end_date ROLLUP_END_DATE
                        this is when rolllup ends (default: None)
-  --rollup_interval_type ROLLUP_INTERVAL_TYPE
                        rollup interval type such as day, month,
                        advertiser_day (default: None)
-  --rollup_name ROLLUP_NAME
                        use if you want to run particular rollup based of it's
                        name for example ROLLUP_ox_transaction_sum (default:
                        None)
-  --rollup_start_date ROLLUP_START_DATE
                        rollup start date the rollup job will queue all the
                        posibile combination for the rollup start date
                        (default: None)
-  --run_rollup_queue RUN_ROLLUP_QUEUE
                        Run rollup from the queue, to run rollup you can
                        provide this by default it is true (default: None)
-  -s START_SERIAL, --start_serial START_SERIAL
                        The start of the range of serials to run for. Will
                        default to the greatest serial the uploader is at.
                        (default: None)
-  -t TYPE, --type TYPE  Report Type (default: delta)
-  -h, --help            show this help message and exit



How to call an action
--------
Download Action
--------
`odfi_etl download -j ox_transaction_sum_and_domain_hourly`

Upload Action
--------
`odfi_etl download -j ox_transaction_sum_and_domain_hourly`

Rollup Action
--------
- To actually run all queued rollups for the job ox_transaction_sum_and_domain_hourly:
> `odfi_etl rollup -j ox_transaction_sum_and_domain_hourly`

- To queue and run a rollup for a datetime (rollup_start_date)
>    `odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date '2013-03-15 08:00:00'`

- To queue rollup dates because something changed in source tables between these dates (dates are inclusive):
>`odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date '2013-03-15 00:00:00' --rollup_end-date '2013-03-16 00:00:00'`

- To queue and then run rollup dates for particular rollup_interval_type(day/month/advertiser_day/platform_day) if not provided, rollup will run for all:
>`odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date '2013-03-15 00:00:00' --rollup_end-date '2013-03-16 00:00:00" \
--rollup_name ROLLUP_ox_transaction_sum  --rollup_interval_type day`

- To queue and then run rollup for particular rollup, a job can have multiple rollup for example ROLLUP_ox_transaction_sum and ROLLUP_domain_hourly:
>`odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date '2013-03-15 00:00:00' --rollup_end-date '2013-03-16 00:00:00" \
--rollup_name ROLLUP_ox_transaction_sum  --rollup_interval_type day --rollup_name ROLLUP_ox_transaction_sum`

- To preview the queue for the rollup without running the rollup, it will log the information
>`odfi_etl rollup -j ox_transaction_sum_and_domain_hourly --rollup_start-date "2013-03-15 00:00:00" --rollup_end-date "2013-03-16 00:00:00" --preview_rollup_queue`

- The above commands will will calculate and queue all the affected hours the hour(s) changed such the day containing that hour in all platform will get re-rolled.
Depending whether queue_next_rollup is set, it will queue the next rollup accordingly.

- NOTE that a queued job (espcially monthly rollup jobs) can get run before the base data (i.e. daily)
is available. When that happens, that job will do nothing and removed from its queue. When the base data
is available, either the same rollup job will get queued again or that base job call invoke the rollup
job directly.  As for load_state, it will only be updated if the base data is more current.

Special Rollup Cases
-----
There are cases when there is one job done you want to rollup other job tables for that hours. 

for example in case of supply_demand_geo_conv job, we do queue the rollup for video_demand. 

To handle this case we have defined 

```
ROLLUP_CONFIG:
  QUEUE_DEPENDENT_ROLLUP_JOB:
    - video_demand
```
in supply_demand_geo_conv.yaml config


Adjustment Action
--------
