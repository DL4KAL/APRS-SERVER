name: aprs-collector

on:
  schedule:
    - cron: "0 * * * *"
  workflow_dispatch:

concurrency:
  group: aprs
  cancel-in-progress: false

jobs:
  collect:
    runs-on: ubuntu-latest
    timeout-minutes: 6
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - run: node collector/collector.mjs
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
          APRS_CALL: ${{ secrets.APRS_CALL }}
          APRS_PASS: ${{ secrets.APRS_PASS }}
