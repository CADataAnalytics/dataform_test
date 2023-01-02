const scd = require("dataform-scd");


const { updates, view } = scd("GOLDENGATE_SCD", {
  uniqueKey: "tenantId",
  timestamp: "publish_time",
  source: {
    schema: "raw_event",
    name: "GOLDENGATE_BANK_STATEMENT_CREATED",
  },
  tags: ["slowly-changing-dimensions"],
  columns: {tenantId: "User ID", publish_time: "Timestamp for updates"},
  incrementalConfig: {
    bigquery: {
      partitionBy: "publish_time",
    },
  },
});

updates.config({
  description: "Updates table for SCD",
});